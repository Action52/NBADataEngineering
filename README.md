# NBADataEngineering
Capstone project for the Udacity Data Engineering Nanodegree. In this project, we perform an ETL process on a set of NBA datasets.  

As a huge NBA fan, I'd like to extract and organise some data and insights from my favourite teams
and players. I will reorder the csvs in such a way that I can easily query for augmented information that combines the 
data from the original sources. By doing this, the resulting database will be an excellent example of an Analytics table.
The data will be extracted from 5 different csvs: 
- **historical_nba_performance.csv**  
This dataset contains information about team performance per year.
- **nba_all_star_games.csv**  
This dataset includes information about the players participating in the yearly All-Star games.
- **nba_shots_2000_to_2018.csv**
This dataset (the biggest one with over 1m rows) shows a detailed description of all the shots made from 2000 to 2018 on every game.
- **player_data.csv**  
Information about each player.
- **players.csv**
More informacion about players.  

### Installation
First things first. To correctly run the project, please install the dependencies on an empty python environment.
```
# Run this on the project's source folder. I am using conda in this example.
conda create --name dec python=3.7 --no-default-packages
pip install -e .
```


### Exploratory Data Analysis


```python
import pandas as pd

# Setting the chunksize to 100 since we just want to take a first look into the data ;) 
historical = pd.read_csv("data/historical_nba_performance.csv", chunksize=100).get_chunk()
all_star = pd.read_csv("data/all_star.csv", chunksize=100).get_chunk()
shots = pd.read_csv("data/NBA_Shots_2000_to_2018.csv", chunksize=100).get_chunk()
player_data_1 = pd.read_csv("data/player_data.csv", chunksize=100).get_chunk()
player_data_2 = pd.read_csv("data/Players.csv", chunksize=100).get_chunk()
```

#### Historical Teams table


```python
historical.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Year</th>
      <th>Team</th>
      <th>Record</th>
      <th>Winning Percentage</th>
      <th>Unnamed: 4</th>
      <th>Unnamed: 5</th>
      <th>Unnamed: 6</th>
      <th>Unnamed: 7</th>
      <th>Unnamed: 8</th>
      <th>Unnamed: 9</th>
      <th>Unnamed: 10</th>
      <th>Unnamed: 11</th>
      <th>Unnamed: 12</th>
      <th>Unnamed: 13</th>
      <th>Unnamed: 14</th>
      <th>Unnamed: 15</th>
      <th>Unnamed: 16</th>
      <th>Unnamed: 17</th>
      <th>Unnamed: 18</th>
      <th>Unnamed: 19</th>
      <th>Unnamed: 20</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2016-17</td>
      <td>Celtics</td>
      <td>25-15</td>
      <td>0.625</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2015-16</td>
      <td>Celtics</td>
      <td>48-34</td>
      <td>0.585</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2014-15</td>
      <td>Celtics</td>
      <td>40-42</td>
      <td>0.488</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2013-14</td>
      <td>Celtics</td>
      <td>25-57</td>
      <td>0.305</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2012-13</td>
      <td>Celtics</td>
      <td>41-40</td>
      <td>0.506</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



Ok, so apparently this csv has 4 columns and a number of unnamed columns that were probably reserved for extra values. The columns are:
- Year
- Team
- Record
- Winning Percentage

#### All-Star teams table


```python
all_star.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Year</th>
      <th>Player</th>
      <th>Pos</th>
      <th>HT</th>
      <th>WT</th>
      <th>Team</th>
      <th>Selection Type</th>
      <th>NBA Draft Status</th>
      <th>Nationality</th>
      <th>Unnamed: 9</th>
      <th>Unnamed: 10</th>
      <th>Unnamed: 11</th>
      <th>Unnamed: 12</th>
      <th>Unnamed: 13</th>
      <th>Unnamed: 14</th>
      <th>Unnamed: 15</th>
      <th>Unnamed: 16</th>
      <th>Unnamed: 17</th>
      <th>Unnamed: 18</th>
      <th>Unnamed: 19</th>
      <th>Unnamed: 20</th>
      <th>Unnamed: 21</th>
      <th>Unnamed: 22</th>
      <th>Unnamed: 23</th>
      <th>Unnamed: 24</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2016</td>
      <td>Stephen Curry</td>
      <td>G</td>
      <td>6-3</td>
      <td>190</td>
      <td>Golden State Warriors</td>
      <td>Western All-Star Fan Vote Selection</td>
      <td>2009 Rnd 1 Pick 7</td>
      <td>United States</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2016</td>
      <td>James Harden</td>
      <td>SG</td>
      <td>6-5</td>
      <td>220</td>
      <td>Houston Rockets</td>
      <td>Western All-Star Fan Vote Selection</td>
      <td>2009 Rnd 1 Pick 3</td>
      <td>United States</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2016</td>
      <td>Kevin Durant</td>
      <td>SF</td>
      <td>6-9</td>
      <td>240</td>
      <td>Golden State Warriors</td>
      <td>Western All-Star Fan Vote Selection</td>
      <td>2007 Rnd 1 Pick 2</td>
      <td>United States</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2016</td>
      <td>Kawhi Leonard</td>
      <td>F</td>
      <td>6-7</td>
      <td>230</td>
      <td>San Antonio Spurs</td>
      <td>Western All-Star Fan Vote Selection</td>
      <td>2011 Rnd 1 Pick 15</td>
      <td>United States</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2016</td>
      <td>Anthony Davis</td>
      <td>PF</td>
      <td>6-11</td>
      <td>253</td>
      <td>New Orleans Pelicans</td>
      <td>Western All-Star Fan Vote Selection</td>
      <td>2012 Rnd 1 Pick 1</td>
      <td>United States</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>




```python
print([col for col in all_star.columns if 'Unnamed' not in col ])
```

    ['Year', 'Player', 'Pos', 'HT', 'WT', 'Team', 'Selection Type', 'NBA Draft Status', 'Nationality']


#### Nba Shots


```python
shots.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Unnamed: 0</th>
      <th>X</th>
      <th>ID</th>
      <th>Player</th>
      <th>Season</th>
      <th>Top.px. (Location)</th>
      <th>Left.px. (location)</th>
      <th>Date</th>
      <th>Team</th>
      <th>Opponent</th>
      <th>Location</th>
      <th>Quarter</th>
      <th>Game_Clock</th>
      <th>Outcome (1 if made, 0 otherwise)</th>
      <th>Shot_Value</th>
      <th>Shot_Distance.ft.</th>
      <th>Team_Score</th>
      <th>Opponent_Score</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>abdulma02</td>
      <td>Mahmoud Abdul-Rauf</td>
      <td>2001</td>
      <td>250</td>
      <td>304</td>
      <td>110600</td>
      <td>VAN</td>
      <td>ATL</td>
      <td>HOME</td>
      <td>3</td>
      <td>00:38.4</td>
      <td>0</td>
      <td>2</td>
      <td>21</td>
      <td>69</td>
      <td>55</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>2</td>
      <td>abdulma02</td>
      <td>Mahmoud Abdul-Rauf</td>
      <td>2001</td>
      <td>147</td>
      <td>241</td>
      <td>111800</td>
      <td>VAN</td>
      <td>DAL</td>
      <td>HOME</td>
      <td>2</td>
      <td>9:22</td>
      <td>1</td>
      <td>2</td>
      <td>10</td>
      <td>33</td>
      <td>26</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>3</td>
      <td>abdulma02</td>
      <td>Mahmoud Abdul-Rauf</td>
      <td>2001</td>
      <td>132</td>
      <td>403</td>
      <td>112400</td>
      <td>VAN</td>
      <td>DET</td>
      <td>AWAY</td>
      <td>3</td>
      <td>6:42</td>
      <td>0</td>
      <td>2</td>
      <td>18</td>
      <td>60</td>
      <td>78</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>4</td>
      <td>abdulma02</td>
      <td>Mahmoud Abdul-Rauf</td>
      <td>2001</td>
      <td>177</td>
      <td>129</td>
      <td>112400</td>
      <td>VAN</td>
      <td>DET</td>
      <td>AWAY</td>
      <td>3</td>
      <td>2:42</td>
      <td>0</td>
      <td>2</td>
      <td>17</td>
      <td>66</td>
      <td>80</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>5</td>
      <td>abdulma02</td>
      <td>Mahmoud Abdul-Rauf</td>
      <td>2001</td>
      <td>99</td>
      <td>390</td>
      <td>112400</td>
      <td>VAN</td>
      <td>DET</td>
      <td>AWAY</td>
      <td>3</td>
      <td>2:18</td>
      <td>0</td>
      <td>2</td>
      <td>16</td>
      <td>66</td>
      <td>80</td>
    </tr>
  </tbody>
</table>
</div>




```python
print([col for col in shots.columns if 'Unnamed' not in col ])
```

    ['X', 'ID', 'Player', 'Season', 'Top.px. (Location)', 'Left.px. (location)', 'Date', 'Team', 'Opponent', 'Location', 'Quarter', 'Game_Clock', 'Outcome (1 if made, 0 otherwise)', 'Shot_Value', 'Shot_Distance.ft.', 'Team_Score', 'Opponent_Score']


The info on this table makes up for a great facts table.

#### players_data and Players table


```python
player_data_1.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>year_start</th>
      <th>year_end</th>
      <th>position</th>
      <th>height</th>
      <th>weight</th>
      <th>birth_date</th>
      <th>college</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Alaa Abdelnaby</td>
      <td>1991</td>
      <td>1995</td>
      <td>F-C</td>
      <td>6-10</td>
      <td>240</td>
      <td>June 24, 1968</td>
      <td>Duke University</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Zaid Abdul-Aziz</td>
      <td>1969</td>
      <td>1978</td>
      <td>C-F</td>
      <td>6-9</td>
      <td>235</td>
      <td>April 7, 1946</td>
      <td>Iowa State University</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Kareem Abdul-Jabbar</td>
      <td>1970</td>
      <td>1989</td>
      <td>C</td>
      <td>7-2</td>
      <td>225</td>
      <td>April 16, 1947</td>
      <td>University of California, Los Angeles</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Mahmoud Abdul-Rauf</td>
      <td>1991</td>
      <td>2001</td>
      <td>G</td>
      <td>6-1</td>
      <td>162</td>
      <td>March 9, 1969</td>
      <td>Louisiana State University</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Tariq Abdul-Wahad</td>
      <td>1998</td>
      <td>2003</td>
      <td>F</td>
      <td>6-6</td>
      <td>223</td>
      <td>November 3, 1974</td>
      <td>San Jose State University</td>
    </tr>
  </tbody>
</table>
</div>




```python
player_data_2.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Unnamed: 0</th>
      <th>Player</th>
      <th>height</th>
      <th>weight</th>
      <th>collage</th>
      <th>born</th>
      <th>birth_city</th>
      <th>birth_state</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Curly Armstrong</td>
      <td>180</td>
      <td>77</td>
      <td>Indiana University</td>
      <td>1918</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>Cliff Barker</td>
      <td>188</td>
      <td>83</td>
      <td>University of Kentucky</td>
      <td>1921</td>
      <td>Yorktown</td>
      <td>Indiana</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Leo Barnhorst</td>
      <td>193</td>
      <td>86</td>
      <td>University of Notre Dame</td>
      <td>1924</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>Ed Bartels</td>
      <td>196</td>
      <td>88</td>
      <td>North Carolina State University</td>
      <td>1925</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>Ralph Beard</td>
      <td>178</td>
      <td>79</td>
      <td>University of Kentucky</td>
      <td>1927</td>
      <td>Hardinsburg</td>
      <td>Kentucky</td>
    </tr>
  </tbody>
</table>
</div>




```python
print([col for col in player_data_1.columns if 'Unnamed' not in col ])
print([col for col in player_data_2.columns if 'Unnamed' not in col ])
```

    ['name', 'year_start', 'year_end', 'position', 'height', 'weight', 'birth_date', 'college']
    ['Player', 'height', 'weight', 'collage', 'born', 'birth_city', 'birth_state']


As you can notice, both tables contain similar data, so in the architecture we will make sure to combine this tables with their useful info.

### Proposed Data Model

Given the nature of the data, I am going to use a Star schema to create a RedshiftDB with 4 nodes. The idea is to host the player dimensions and the team dimensions on each node (distribution type ALL), and the shots fact table will be distributed across all 4 nodes. This will allow us to expand the database horizontally if needed, and also it will allow us to access the data faster since each node will contain the necessary information.

![](data/images/star.png)

Note that the players table will extrapolate some info from the "all_star" original csv, in particular the nba_draft_status and the nationality of the player.  

This architecture will let us extract and augment useful information regarding the shots made.
Possible queries to answer:

- Teams with the most cumulative points scored per season.
- Players with the most cumulative points scored.
- Players with "clutch" (this means that they are able to score in the last 2 minutes of the game, in the 4th quarter).
- Players with the best 3 point scoring percentage.
- Extract the colleges with the most winning players (players who have won the most games).


```python

```
