// acme_audit: append-only event log referencing customers in acme_orders.
CREATE (:Event {id: 'E1', customer_id: 'C001', action: 'LOGIN', at: datetime()})
CREATE (:Event {id: 'E2', customer_id: 'C001', action: 'PLACE_ORDER', at: datetime()})
CREATE (:Event {id: 'E3', customer_id: 'C002', action: 'PLACE_ORDER', at: datetime()});
