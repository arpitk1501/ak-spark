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

// Join tables

CREATE TABLE tutorial.address
(
    name         text primary key,
    address_line text,
    city         text
);


CREATE TABLE tutorial.user_address
(
    name    text primary key,
    food    text,
    address text,
    concat  text

);

create table tutorial.office
(
    office_name text primary key,
    office_city text
);

insert into tutorial.office(office_name, office_city) VALUES ('Egl', 'Bangalore');
insert into tutorial.office(office_name, office_city) VALUES ('PTP', 'Bangalore');
insert into tutorial.office(office_name, office_city) VALUES ('ABC', 'Delhi');
insert into tutorial.office(office_name, office_city) VALUES ('DLF', 'Gurgaon');

create table tutorial.user_office_details
(
    name        text,
    office_name text,
    designation text,
    salary      decimal,
    primary key (name, office_name)
);

insert into tutorial.address(name, address_line, city)
values ('Jon', 'B-104', 'Bangalore');


insert into tutorial.address(name, address_line, city)
values ('Luke', 'A-1', 'Delhi');


insert into tutorial.address(name, address_line, city)
values ('Al', 'B-2', 'Mumbai');


insert into tutorial.address(name, address_line, city)
values ('Chris', 'D-12', 'Agra');


insert into tutorial.address(name, address_line, city)
values ('Rebecca', 'B-104', 'Bangalore');

