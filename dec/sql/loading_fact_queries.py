def insert_fact_shots(table, raw_table, dim_player, dim_all_star, dim_historical):
    return f"""
        INSERT INTO {table} (
            player_id,
            all_star_id,
            season,
            top_px_location,
            left_px_location,
            date,
            team,
            opponent,
            location,
            quarter,
            game_clock,
            converted,
            shot_value,
            shot_distance_ft,
            team_score,
            opponent_score
        )
        SELECT
            dp.player_id AS player_id,
            ds.all_star_id AS all_star_id,
            r.season AS season,
            CAST(r.top_px_location AS INT),
            CAST(r.left_px_location AS INT),
            TO_DATE(r.date, 'MMDDYY') AS date,
            r.team,
            r.opponent,
            r.location,
            r.quarter,
            r.game_clock,
            CASE WHEN CAST(r.outcome_1_if_made_0_otherwise AS INT) = 0 THEN FALSE ELSE TRUE END AS converted,
            r.shotvalue AS shot_value,
            CAST(r.shotdistance_ft AS INT) AS shot_distance_ft,
            CAST(r.team_score AS INT),
            CAST(r.opponentscore AS INT) AS opponent_score
        FROM {raw_table} r 
            JOIN {dim_player} dp ON r.player = dp.player_name 
            LEFT JOIN {dim_all_star} ds ON r.player = ds.player_name 
            LEFT JOIN {dim_historical} dh ON (
                r.team = dh.team_abbr AND 
                CAST(SUBSTRING(r.season, 3, 2) AS INT) = dh.season_end_year
            );
    """
