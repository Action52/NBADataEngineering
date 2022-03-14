def create_raw_shots_table(table):
    return f"""
            CREATE TABLE IF NOT EXISTS {table}
                (
                    x                               VARCHAR,
                    id                              VARCHAR,
                    player                          VARCHAR,
                    season                          VARCHAR,
                    top_px_location                 VARCHAR,
                    left_px_location                VARCHAR,
                    date                            VARCHAR,
                    team                            VARCHAR,
                    opponent                        VARCHAR,
                    location                        VARCHAR,
                    quarter                         VARCHAR,
                    game_clock                      VARCHAR,
                    outcome_1_if_made_0_otherwise   VARCHAR,
                    shotvalue                       VARCHAR,
                    shotdistance_ft                 VARCHAR,
                    team_score                      VARCHAR,
                    opponentscore                   VARCHAR
                ) DISTSTYLE EVEN;
    """


def create_raw_players_1_table(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table}
            (
                name            VARCHAR,
                year_start      VARCHAR,
                year_end        VARCHAR,
                position        VARCHAR,
                height          VARCHAR,
                weight          VARCHAR,
                birth_date      VARCHAR,
                college         VARCHAR
            ) DISTSTYLE ALL;
    """


def create_raw_players_2_table(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table}
            (
                player          VARCHAR,
                height          VARCHAR,
                weight          VARCHAR,
                collage         VARCHAR,
                born            VARCHAR,
                birth_city      VARCHAR,
                birth_state     VARCHAR
            ) DISTSTYLE ALL;
    """


def create_raw_all_star_table(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table}
            (
                year                VARCHAR,
                player              VARCHAR,
                pos                 VARCHAR,
                ht                  VARCHAR,
                wt                  VARCHAR,
                team                VARCHAR,
                selection_type      VARCHAR,
                nba_draft_status    VARCHAR,
                nationality         VARCHAR
            ) DISTSTYLE ALL;
    """


def create_raw_teams_table(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table}
            (
                year                VARCHAR,
                team                VARCHAR,
                record              VARCHAR,
                winning_percentage  VARCHAR,
                abbr                VARCHAR
            ) DISTSTYLE ALL;
    """


def create_fact_shots(table, player_table, historical_table, all_star_table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table}(
            shot_id                 INT IDENTITY(0,1),
            player_id               INT NOT NULL REFERENCES {player_table}(player_id),
            all_star_id             INT REFERENCES {all_star_table}(all_star_id),
            season                  VARCHAR,
            top_px_location         INT,
            left_px_location        INT,
            date                    DATE,
            team                    VARCHAR(3),
            opponent                VARCHAR(3),
            location                VARCHAR(4),
            quarter                 VARCHAR(4),
            game_clock              VARCHAR(10),
            converted               BOOLEAN DEFAULT FALSE,
            shot_value              VARCHAR(3),
            shot_distance_ft        INT,
            team_score              INT,
            opponent_score          INT,
            PRIMARY KEY (shot_id)
        ) DISTSTYLE EVEN;
    """


def create_dimension_historical(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            team_id                 INT IDENTITY(0,1),
            team_name               VARCHAR(50),
            team_abbr               VARCHAR(3),
            season_start_year       INT,
            season_end_year         INT,
            games_won               INT,
            games_lost              INT,
            winning_percentage      FLOAT,
            PRIMARY KEY(team_id)  
        ) DISTSTYLE ALL;
    """


def create_dimension_player(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            player_id               INT IDENTITY(0,1),
            player_name             VARCHAR(100),
            position                VARCHAR(20),
            dob                     DATE,
            height                  FLOAT,
            weight                  FLOAT,
            year_start              INT,
            year_end                INT,
            college                 VARCHAR(100),
            nba_draft_status        VARCHAR(100),
            nationality             VARCHAR(50),
            PRIMARY KEY(player_id)
        ) DISTSTYLE ALL;
    """


def create_dimension_all_star_games(table):
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            all_star_id             INT IDENTITY(0,1),
            year                    INT,
            player_name             VARCHAR(100),
            selection_type          VARCHAR(150),
            PRIMARY KEY(all_star_id)
        ) DISTSTYLE ALL;
    """
