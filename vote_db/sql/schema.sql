CREATE TABLE IF NOT EXISTS vote_transactions
(
    id           INTEGER        PRIMARY KEY AUTOINCREMENT,
    txhash       TEXT           UNIQUE NOT NULL,
    block        INTEGER        NOT NULL,
    timestamp    NUMERIC        NOT NULL,
    [from]       TEXT           NOT NULL,
    data         TEXT           NOT NULL
);

CREATE TABLE IF NOT EXISTS stake_transactions
(
    id          INTEGER         PRIMARY KEY AUTOINCREMENT,
    txhash       TEXT           UNIQUE NOT NULL,
    block        INTEGER        NOT NULL,
    timestamp    NUMERIC        NOT NULL,
    [from]       TEXT           NOT NULL,
    data         TEXT           NOT NULL
)