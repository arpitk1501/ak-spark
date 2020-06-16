CREATE KEYSPACE cc_engine WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use cc_engine;

create table cc_engine.legal_entity
(
    le_id      text,
    le_name    text,
    le_type    text, //CP/CR
    created_by text,
    created_ts timestamp,
    PRIMARY KEY ( le_id )
);

create table cc_engine.account
(
    account_id   text,
    account_name text,
    account_type text,
    le_id        text,
    created_by   text,
    created_ts   timestamp,
    PRIMARY KEY ( le_id, account_id )
);


create table cc_engine.asset_composite
(
    asset_id      text,
    asset_name    text,
    country_code  text,
    currency_code text,
    market_code   text,
    price         decimal,
    PRIMARY KEY ( asset_id )
);

create table cc_engine.position
(
    asset_id   text,
    account_id text,
    quantity   decimal,
    PRIMARY KEY ( account_id, asset_id )
) with clustering order by (asset_id asc);

