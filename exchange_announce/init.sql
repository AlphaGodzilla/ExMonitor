-- auto-generated definition
create table new_listing_symbol
(
    symbol         VARCHAR(50)             not null,
    exchange       varchar(255)            not null,
    new_listing_at BIGINT                  not null,
    url            varchar(255) default '' not null,
    last_update_at bigint       default 0  not null,
    created_at     bigint       default 0  not null,
    title          varchar(255) default '' not null,
    constraint new_listing_symbol_pk
        unique (symbol, exchange)
);

create index idx_created_at
    on new_listing_symbol (created_at desc);

create index idx_time_exchange
    on new_listing_symbol (new_listing_at, exchange);

