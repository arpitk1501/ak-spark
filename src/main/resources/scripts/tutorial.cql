CREATE KEYSPACE tutorial WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use tutorial;

CREATE TABLE tutorial.user
(
    name          text primary key,
    favorite_food text
);

create table tutorial.food_to_user_index
(
    food text,
    user text,
    primary key (food, user)
);

insert into user (name, favorite_food)
values ('Jon', 'bacon');
insert into user (name, favorite_food)
values ('Luke', 'steak');
insert into user (name, favorite_food)
values ('Al', 'salmon');
insert into user (name, favorite_food)
values ('Chris', 'chicken');
insert into user (name, favorite_food)
values ('Rebecca', 'bacon');
insert into user (name, favorite_food)
values ('Patrick', 'brains');
insert into user (name, favorite_food)
values ('Duy Hai', 'brains');

select *
from tutorial.user;

truncate table tutorial.food_to_user_index;