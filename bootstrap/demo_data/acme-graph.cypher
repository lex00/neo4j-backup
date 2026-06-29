// acme_graph: product relationships referenced by orders in acme_orders.
CREATE (p1:Product {id: 'P1', name: 'Analytical Engine'})
CREATE (p2:Product {id: 'P2', name: 'Difference Engine'})
CREATE (p3:Product {id: 'P3', name: 'Bombe'})
CREATE (p1)-[:RELATED_TO]->(p2)
CREATE (p3)-[:RELATED_TO]->(p1);
