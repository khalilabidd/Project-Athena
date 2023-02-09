import pandas as pd
import sqlalchemy as sa
import matplotlib.pyplot as plt
from matches import *


def analysePlayer(engine,profile_id):
	"""
	Create a view by joining leaderboard and matches.
	extract player matches and create some graphes to analyse player's winrate and playrate of different civs
	Parameters
	----------
	engine : sqlalchemy.engine.Engine
		The SQLAlchemy Engine to use.
	player_id : int
		id of the player
	"""
	with engine.begin() as conn:

		# Create a view by joining leaderboard and matches.
		conn.exec_driver_sql("""CREATE OR REPLACE view leaderboards_matches as (
			select leaderboard.profile_id, leaderboard.rank, leaderboard.name, leaderboard.country, 
			leaderboard.rating as currentrating, leaderboard.games, leaderboard.wins,leaderboard.losses, leaderboard.drops, 
			matches.match_id, matches.rating,matches.civ, matches.map_type, matches.started, matches.finished, matches.won,
			matches.opponent, matches.opponent_rating, matches.opponent_civ, matches.opponent_id 
			from leaderboard join matches on leaderboard.profile_id=matches.profile_id)""")

		df_leaderboards_matches = pd.read_sql(sa.text("""select * from leaderboards_matches"""),conn)
		print(df_leaderboards_matches.head())

		# load player's matches history from matches in chronological order
		df_player_matches = pd.read_sql(sa.text(f"select * from matches where profile_id={profile_id} order by started"),conn)

	# replacing ratings missing value with the previous values
	df_player_matches.rating = df_player_matches.rating.fillna(method='ffill')

	# plot player's ratings history
	df_player_matches.plot(x='started',y='rating',figsize= (10,5),xlabel='date',title=f' AoE2 historical player rating of {df_player_matches.name.iloc[0]}')
	plt.savefig(f'{df_player_matches.name.iloc[0]}_historicalrating')

	# calculating winrate and playrate of each civs
	won = df_player_matches.loc[df_player_matches.won==True,'civ'].value_counts()
	civ_stats = df_player_matches[['civ']].value_counts().rename_axis('civ').reset_index(name='counts')
	civ_stats['winrate'] = civ_stats.apply(lambda x: won[x.civ]/x.counts, axis = 1)
	civ_stats['playrate'] = civ_stats.apply(lambda x: x.counts/civ_stats.counts.sum(), axis = 1)

	# plot player's civ winrate vs playrate.
	ax = civ_stats.plot(x='playrate',y='winrate',figsize= (10,6),kind='scatter',grid=True,title=f'Civ Win Rate vs Play Rate of {df_player_matches.name.iloc[0]}')
	for _, v in civ_stats.iterrows():
		ax.annotate(v.civ, (v.playrate,v.winrate+0.01))
	ax.axhline(0.5, color="red")
	ax.axvline(1/42, color="red")
	plt.savefig(f'{df_player_matches.name.iloc[0]}_civplayratevswinrate')


if __name__ == "__main__":
	profile_id = 199325
	db_user,db_pass,db_host,db_port,db_name='postgres','postgres','postgres','5432','postgres'
	engine = sa.create_engine('postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name))
	analysePlayer(engine,profile_id)