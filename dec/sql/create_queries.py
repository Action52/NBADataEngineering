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
                winning_percentage  VARCHAR
            ) DISTSTYLE ALL;
    """

