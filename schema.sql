-- Teams table
CREATE TABLE IF NOT EXISTS teams (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    abbreviation TEXT NOT NULL UNIQUE
);

-- Players table
CREATE TABLE IF NOT EXISTS players (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    team_id INT REFERENCES teams(id)
);

-- Games table
CREATE TABLE IF NOT EXISTS games (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    home_team_id INT REFERENCES teams(id),
    away_team_id INT REFERENCES teams(id),
    home_score INT,
    away_score INT,
    status TEXT DEFAULT 'scheduled'
);

-- Betting odds table
CREATE TABLE IF NOT EXISTS betting_odds (
    id SERIAL PRIMARY KEY,
    game_id INT REFERENCES games(id),
    bookmaker TEXT NOT NULL,
    moneyline_home INT,
    moneyline_away INT,
    over_under FLOAT
);
