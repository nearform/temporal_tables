\timing on

INSERT INTO subscriptions(name, state) SELECT 'test' || g, 'inserted' FROM generate_series (1,100000) AS t(g);