import pandas as pd
import requests
import sqlalchemy as sa
import pandera as pa

## CONSTANTS 

LEADERBOARD_ID = 3 # corresponding value to 1vs1 ranked matches
MATCH_COUNT = 100 # number of matches to retrieve
ITERATIONS = 15 # number of iteration to call the api since the retrieve number is limited each call
CIVS_URL = "https://docs.google.com/spreadsheets/d/1z25PgHskf6AE6mbwKQahoQrcd9SpOo26U-2ZiUI0SMk/export?format=csv&gid=0"
MAPS_URL = "https://docs.google.com/spreadsheets/d/1QG1MWrOwEp1DRT4uHyjWwoEcdvuKON87yqIpToNUyJU/export?format=csv&gid=0"


## Functions

def dfUpsert(data_frame, table_name, engine, match_columns=None):
	"""
	Perform an "upsert" on a PostgreSQL table from a DataFrame.
	Constructs an INSERT … ON CONFLICT statement, uploads the DataFrame to a
	temporary table, and then executes the INSERT.
	Parameters
	----------
	data_frame : pandas.DataFrame
		The DataFrame to be upserted.
	table_name : str
		The name of the target table.
	engine : sqlalchemy.engine.Engine
		The SQLAlchemy Engine to use.
	match_columns : list of str, optional
		A list of the column name(s) on which to match. If omitted, the
		primary key columns of the target table will be used.
	"""
	table_spec = '"' + table_name.replace('"', '""') + '"'

	df_columns = list(data_frame.columns)
	if not match_columns:
		insp = sa.inspect(engine)
		match_columns = insp.get_pk_constraint(table_name)[
			"constrained_columns"
		]
	columns_to_update = [col for col in df_columns if col not in match_columns]
	insert_col_list = ", ".join([f'"{col_name}"' for col_name in df_columns])
	stmt = f"INSERT INTO {table_spec} ({insert_col_list})\n"
	stmt += f"SELECT {insert_col_list} FROM temp_table\n"
	match_col_list = ", ".join([f'"{col}"' for col in match_columns])
	stmt += f"ON CONFLICT ({match_col_list}) DO UPDATE SET\n"
	stmt += ", ".join(
		[f'"{col}" = EXCLUDED."{col}"' for col in columns_to_update]
	)

	with engine.begin() as conn:
		conn.exec_driver_sql("DROP TABLE IF EXISTS temp_table")
		conn.exec_driver_sql(
			f"CREATE TEMPORARY TABLE temp_table AS SELECT * FROM {table_spec} WHERE false"
		)
		data_frame.to_sql("temp_table", conn, if_exists="append", index=False)
		conn.exec_driver_sql(stmt)


# I used this code in order to generate maps and civs files and place them in the cloud after correcting them
# response_strings = requests.get(f"https://aoe2.net/api/strings?game=aoe2de")
# if response_strings.status_code == 200:
#     df_civs = pd.json_normalize(data = response_strings.json(),record_path = ['civ'])
#     df_maps = pd.json_normalize(data = response_strings.json(),record_path = ['map_type'])
# else:
#     print(f'Failed to retrieve data ({response_leaderboard.status_code}): {response_leaderboard.request.url}')
# maps = dict(zip(df_maps.id, df_maps.string))
# civs = dict(zip(df_civs.id, df_civs.string))
# civs[40] = 'Dravidians'
# civs[41] = 'Bengalis'
# civs[42] = 'Gurjaras'

def retrievePlayerMatchesHistory(engine,profile_id,i,civs,maps):
	"""
	Extract a number of matches from player's match history using aoe2.net API 
	Transform, clean and correct some data
	Then update or insert the DataFrame to the PostgreSQL table matches
	Parameters
	----------
	engine : sqlalchemy.engine.Engine
		The SQLAlchemy Engine to use.
	player_id : int
		id of the player
	i : int
		start value of first match retrieved in api
	civs: dict
		dictionnaries with corresponding name to each civ id
	maps: dict
		dictionnaries with corresponding name to each map_type id
	"""

	# Extract
	response_matches = requests.get(f"https://aoe2.net/api/player/matches?game=aoe2de&profile_id={profile_id}&count={MATCH_COUNT}&start={MATCH_COUNT*i}")
	if response_matches.status_code == 200:
		df_matches = pd.DataFrame(response_matches.json())
		df_matches = pd.json_normalize(
			response_matches.json(),
			record_path =['players'],
			meta=['leaderboard_id','match_id','map_type','started','finished'],
			errors='ignore'
		)


		# Transform

		# Drop unnecessary columns
		df_matches = df_matches.drop(columns=['slot','country','clan','slot_type','team','won'])

		# Filter 1vs1 ranked matches
		df_matches = df_matches[df_matches.leaderboard_id==LEADERBOARD_ID] 
		if len(df_matches)>0:
			df_matches = df_matches.astype(
				{"leaderboard_id": 'int64',
				"profile_id": 'int64', 
				'match_id': 'int64',
				'map_type': 'int64',
				'civ': 'int64',
				'started': 'int64',
				'finished': 'int64',
				'rating_change': 'float64'},
				errors='ignore')

			# quality check
			schema_matches = pa.DataFrameSchema({
				"profile_id": pa.Column(int,nullable=False),
				"name": pa.Column(str,nullable=True),
				"rating": pa.Column(float,nullable=True),
				"rating_change": pa.Column(float,nullable=True),
				"color": pa.Column(int,nullable=True),
				"civ": pa.Column(int,pa.Check.isin(list(civs.keys()))),
				"leaderboard_id": pa.Column(int,nullable=True),
				"match_id": pa.Column(int,nullable=False),
				"map_type": pa.Column(int,pa.Check.isin(list(maps.keys()))),
				"started": pa.Column(int,nullable=True),
				"finished": pa.Column(int,nullable=True),
			})
			try:
				schema_matches.validate(df_matches)
				# removing duplicates to have unique composite primary keys values
				df_matches = df_matches.drop_duplicates(subset=['profile_id','match_id'],keep='first')

				# transform column from epoch integer to datetime format
				df_matches['started'] = pd.to_datetime(df_matches['started'],unit='s')
				df_matches['finished'] = pd.to_datetime(df_matches['finished'],unit='s')

				# replace civs and maps id with their corresponding name
				df_matches['civ'] = df_matches.civ.apply(lambda x: civs[x])
				df_matches['map_type'] = df_matches.map_type.apply(lambda x: maps[x])

				# Adding opponent informations to the same match row in order to have a row for each match instead of two
				df_matches['opponent'] = df_matches.apply(lambda x: df_matches.name[(df_matches.match_id==x.match_id) & (df_matches.profile_id!=profile_id)].iloc[0], axis = 1)
				df_matches['opponent_civ'] = df_matches.apply(lambda x: df_matches.civ[(df_matches.match_id==x.match_id) & (df_matches.profile_id!=profile_id)].iloc[0], axis = 1)
				df_matches['opponent_rating'] = df_matches.apply(lambda x: df_matches.rating[(df_matches.match_id==x.match_id) & (df_matches.profile_id!=profile_id)].iloc[0], axis = 1)
				df_matches['opponent_id'] = df_matches.apply(lambda x: df_matches.profile_id[(df_matches.match_id==x.match_id) & (df_matches.profile_id!=profile_id)].iloc[0], axis = 1)
				df_matches = df_matches[df_matches.profile_id==profile_id]

				# correct some missing value. We used previous ratings and ratings changed in previous matches to improve data quality
				df_matches['rating_correction'] = df_matches['rating'].shift(-1) + df_matches['rating_change'].shift(-1)
				df_matches['rating'] = df_matches.apply(
					lambda x: x.rating_correction if pd.isna(x.rating) else x.rating, axis = 1)
				df_matches['rating_change_correction'] = df_matches['rating'].shift(1) - df_matches['rating']
				df_matches['rating_change'] = df_matches.apply(
					lambda x: x.rating_change_correction if pd.isna(x.rating_change) else x.rating_change, axis = 1)
				df_matches = df_matches.drop(columns=['rating_correction','rating_change_correction'])

				# deduce match result based on rating_change
				df_matches['won'] = df_matches.apply(lambda x: True if x.rating_change>0 else False, axis = 1)

				# Load
				dfUpsert(df_matches, "matches", engine)


			except Exception as e:
				print(e)
	else:
		print(f'Failed to retrieve data ({response_leaderboard.status_code}): {response_leaderboard.request.url}')

def loadPlayerMatches(engine,profile_id,ITERATIONS):
	"""
	Create PostgreSQL table matches if not exists
	Load civs and maps dictionnaries
	Apply many iterations to extract of matches from player's match history using aoe2.net API 
	Parameters
	----------
	engine : sqlalchemy.engine.Engine
		The SQLAlchemy Engine to use.
	player_id : int
		id of the player
	ITERATIONS: int
		number of iterations
	"""

	# Create matches table
	with engine.begin() as conn:
		conn.exec_driver_sql(
			"""CREATE TABLE IF NOT EXISTS matches (
			leaderboard_id int,
			match_id int,
			profile_id int,
			name varchar,
			rating int,
			rating_change int,
			color int,
			civ varchar,
			map_type varchar,
			started timestamp,
			finished timestamp,
			opponent varchar,
			opponent_rating int,
			opponent_civ varchar,
			opponent_id int,
			won boolean,
			primary key (match_id, profile_id))
			"""
		)
	df_civs = pd.read_csv(CIVS_URL)
	df_maps = pd.read_csv(MAPS_URL)
	civs = dict(zip(df_civs.id, df_civs.string))
	maps = dict(zip(df_maps.id, df_maps.string))
	for i in range(ITERATIONS):
		print(f'Extracting Iteration: {i}')
		retrievePlayerMatchesHistory(engine,profile_id,i,civs,maps)

if __name__ == "__main__":
	profile_id = 199325
	db_user,db_pass,db_host,db_port,db_name='postgres','postgres','postgres','5432','postgres'
	engine = sa.create_engine('postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name))
	loadPlayerMatches(engine,profile_id,ITERATIONS)