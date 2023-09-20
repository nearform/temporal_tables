-- Invalid dates
SELECT set_system_time('2022-13-01 22:59:59');
SELECT set_system_time('22-13-01 22:59:59');
SELECT set_system_time('2022-12-99 22:59:59');

-- Invalid time
SELECT set_system_time('2022-01-11 99:59:59');
SELECT set_system_time('2022-01-11 22:99:59');
SELECT set_system_time('2022-01-11 22:59:99');
SELECT set_system_time('2022-01-11 22:59');
SELECT set_system_time('2022-01-11 22');

-- Invalid values
SELECT set_system_time('Invalid string value');
SELECT set_system_time(123);
SELECT set_system_time();
