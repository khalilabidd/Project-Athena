import pandas as pd
import requests
import sqlalchemy as sa
import pandera as pa

## CONSTANTS 

LEADERBOARD_ID = 3 # corresponding value to 1vs1 ranked matches
PLAYER_COUNT = 100  # top players number


## Functions

def loadLeaderboard(engine):
	"""
	Extract top player's stats in 1vs1 ranked games using aoe2.net API 
	Transform, clean and correct some data
	Then insert the DataFrame to the PostgreSQL table leaderboards
	Parameters
	----------
	engine : sqlalchemy.engine.Engine
		The SQLAlchemy Engine to use.
	"""

	# Extract
	response_leaderboard = requests.get(f"https://aoe2.net/api/leaderboard?game=aoe2de&leaderboard_id={LEADERBOARD_ID}&count={PLAYER_COUNT}")
	if response_leaderboard.status_code == 200:
		df_leaderboard = pd.json_normalize(data = response_leaderboard.json(),record_path =['leaderboard'])

		# Transform

		# Drop unnecessary columns
		df_leaderboard = df_leaderboard.drop(columns=['icon','steam_id','clan'])
		df_leaderboard = df_leaderboard.drop_duplicates(subset=['profile_id'],keep='first')

		# quality check
		schema_leaderboard = pa.DataFrameSchema({
			"profile_id": pa.Column(int,nullable=False),
			"rank": pa.Column(int,nullable=True),
			"name": pa.Column(str,nullable=True),
			"country": pa.Column(str,nullable=True),
			"rating": pa.Column(int,nullable=True),
			"previous_rating": pa.Column(int,nullable=True),
			"highest_rating": pa.Column(int,nullable=True),
			"streak": pa.Column(int,nullable=True),
			"lowest_streak": pa.Column(int,nullable=True),
			"highest_streak": pa.Column(int,nullable=True),
			"games": pa.Column(int,nullable=True),
			"wins": pa.Column(int,nullable=True),
			"losses": pa.Column(int,nullable=True),
			"drops": pa.Column(int,nullable=True),
			"last_match_time": pa.Column(int,nullable=True),
		})
		try:
			schema_leaderboard.validate(df_leaderboard)

			# transform column from epoch integer to datetime format
			df_leaderboard['last_match_time'] = pd.to_datetime(df_leaderboard['last_match_time'],unit='s')

			# Create leaderboard table
			with engine.begin() as conn:
				conn.exec_driver_sql("DROP TABLE IF EXISTS leaderboard CASCADE")
				conn.exec_driver_sql("""CREATE TABLE IF NOT EXISTS leaderboard (
					profile_id int primary key, 
					rank int, 
					name varchar,
					country varchar,
					rating int,
					previous_rating int,
					highest_rating int,
					streak int,
					lowest_streak int,
					highest_streak int,
					games int,
					wins int,
					losses int,
					drops int,
					last_match_time timestamp)""")

				# Load
				df_leaderboard.to_sql('leaderboard',conn,if_exists='append',index=False)
		except Exception as e:
			print(e)
	else:
		print(f'Failed to retrieve data ({response_leaderboard.status_code}): {response_leaderboard.request.url}')


if __name__ == "__main__":
	db_user,db_pass,db_host,db_port,db_name='postgres','postgres','postgres','5432','postgres'
	engine = sa.create_engine('postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name))
	print('Retrieving leaderboard')
	loadLeaderboard(engine)