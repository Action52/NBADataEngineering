def insert_dim_historical(raw_table, raw_table2, dim_table):
    return f"""
        INSERT INTO {dim_table} (
            team_name, 
            team_abbr, 
            season_start_year, 
            season_end_year, 
            games_won, 
            games_lost, 
            winning_percentage
        )
        SELECT
            team AS team_name,
            abbr as team_abbr,
            CAST(SUBSTRING(year, 3, 2) AS INT) AS season_start_year,
            CAST(SUBSTRING(year, 6, 2) AS INT) AS season_end_year,
            CAST(SUBSTRING(record, 1, 2) AS INT) AS games_won,
            CAST(SUBSTRING(record, 4, 2) AS INT) AS games_lost,
            CAST(winning_percentage AS FLOAT)
        FROM {raw_table}     
    """


def insert_dim_all_star(raw_table, raw_table2, dim_table):
    return f"""
        INSERT INTO {dim_table} (
            year,
            player_name,
            selection_type
        )
        SELECT 
            CAST(year AS INT),
            player AS player_name,
            selection_type
        FROM {raw_table}
    """


def insert_dim_players(raw_table1, raw_table2, dim_table):
    return f"""
        INSERT INTO {dim_table} (
            player_name,
            position,
            dob,
            height,
            weight,
            year_start,
            year_end,
            college               
        )
        SELECT
            CASE
                WHEN p1.name IS NOT NULL THEN p1.name ELSE p2.player
            END AS player_name,
            p1.position AS position,
            TO_DATE(p1.birth_date, 'Month DD, YYYY') AS dob,
            CAST(p2.height AS FLOAT) AS height,
            CAST(p2.weight AS FLOAT) AS weight,
            CAST(p1.year_start AS INT) AS year_start,
            CAST(p1.year_end AS INT) AS year_end,
            p1.college AS college
        FROM 
            {raw_table1} p1 FULL JOIN {raw_table2} p2 
                ON p1.name = p2.player;
    """
