CREATE TABLE IF NOT EXISTS vote_transactions
(
    txhash       TEXT           PRIMARY KEY,
    [from]       TEXT           NOT NULL,
    [to]         TEXT		NOT NULL,
    method	 TEXT		NOT NULL,
    data         TEXT           NOT NULL,
    timestamp    NUMERIC        NOT NULL,
    block        INTEGER        NOT NULL    
);

CREATE TABLE IF NOT EXISTS stake_transactions
(
    txhash       TEXT           PRIMARY KEY,
    [from]       TEXT           NOT NULL,
    [to]         TEXT		NOT NULL,
    method	 TEXT		NOT NULL,
    data         TEXT           NOT NULL,
    timestamp    NUMERIC        NOT NULL,
    block        INTEGER        NOT NULL
);

CREATE TABLE IF NOT EXISTS claimiscore_transactions
(
    txhash       TEXT           PRIMARY KEY,
    [from]       TEXT           NOT NULL,
    [to]         TEXT		NOT NULL,
    method	 TEXT		NOT NULL,
    data         TEXT           NOT NULL,
    timestamp    NUMERIC        NOT NULL,
    block        INTEGER        NOT NULL
);
