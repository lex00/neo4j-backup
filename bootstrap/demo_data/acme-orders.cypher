// acme_orders: customers and the orders they placed.
// Shares customer ids with acme_audit (logical cross-db reference, not a hard rel).
CREATE (c1:Customer {id: 'C001', name: 'Ada Lovelace'})
CREATE (c2:Customer {id: 'C002', name: 'Alan Turing'})
CREATE (o1:Order {id: 'O100', total: 42.00, placed_at: datetime()})
CREATE (o2:Order {id: 'O101', total: 99.50, placed_at: datetime()})
CREATE (c1)-[:PLACED]->(o1)
CREATE (c2)-[:PLACED]->(o2);
