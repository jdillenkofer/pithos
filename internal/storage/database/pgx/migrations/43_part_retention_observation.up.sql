-- Discard mutation-time timestamps from the prototype; GC observes committed zeros.
UPDATE part_registry SET unreferenced_at = NULL;
