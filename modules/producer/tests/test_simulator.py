"""
Tests for the EventSimulator.
"""

from retailsense_producer.simulator import AgeRange, EventSimulator, Gender, SensorEvent


class TestEventSimulator:
    """Test the EventSimulator class."""

    def test_initialization(self):
        """Test simulator initializes correctly."""
        simulator = EventSimulator()
        assert len(simulator.stores) == 3
        assert "ZRH_01" in simulator.stores
        assert "BER_01" in simulator.stores
        assert "MAD_01" in simulator.stores

    def test_generate_event_structure(self):
        """Test that generated event has correct structure."""
        simulator = EventSimulator()
        event = simulator.generate_event()

        assert isinstance(event, SensorEvent)
        assert event.event_id is not None
        assert event.timestamp > 0
        assert event.store_id in simulator.stores
        assert event.zone_id in simulator.zones[event.store_id]
        assert event.sensor_id.startswith("SENSOR_")

        valid_age_ranges = [
            AgeRange.UNDER_18,
            AgeRange.AGE_18_25,
            AgeRange.AGE_26_35,
            AgeRange.AGE_36_50,
            AgeRange.OVER_50,
        ]
        valid_genders = [Gender.MALE, Gender.FEMALE, Gender.UNKNOWN]

        assert event.age_range in valid_age_ranges
        assert event.gender in valid_genders
        assert 0 <= event.dwell_time_seconds <= 3600

    def test_generate_event_uniqueness(self):
        """Test that generated events have unique IDs."""
        simulator = EventSimulator()
        events = simulator.generate_batch(100)

        ids = [e.event_id for e in events]
        assert len(ids) == len(set(ids)), "Event IDs should be unique"

    def test_demographics_distribution(self):
        """Test that demographics follow expected distribution (statistical test)."""
        simulator = EventSimulator()
        events = simulator.generate_batch(1000)

        age_counts = {}
        gender_counts = {}

        for event in events:
            age_counts[event.age_range] = age_counts.get(event.age_range, 0) + 1
            gender_counts[event.gender] = gender_counts.get(event.gender, 0) + 1

        # Check that all age ranges appear (with 1000 samples, unlikely to miss any)
        assert len(age_counts) == 5, "All age ranges should appear in large sample"

        # Check that all genders appear
        assert len(gender_counts) == 3, "All genders should appear in large sample"

    def test_dwell_time_distribution(self):
        """Test that dwell times follow exponential distribution (mostly short)."""
        simulator = EventSimulator()
        events = simulator.generate_batch(1000)

        dwell_times = [e.dwell_time_seconds for e in events]
        avg_dwell = sum(dwell_times) / len(dwell_times)

        # Average should be around 10 seconds (with some variance)
        assert 5 < avg_dwell < 15, (
            f"Average dwell time should be ~10s, got {avg_dwell:.2f}s"
        )

        # Most events should be short (< 30s)
        short_events = sum(1 for t in dwell_times if t < 30)
        assert short_events > 800, "Most events should be short (< 30s)"

    def test_batch_generation(self):
        """Test batch generation returns correct number of events."""
        simulator = EventSimulator()
        batch = simulator.generate_batch(50)

        assert len(batch) == 50
        assert all(isinstance(e, SensorEvent) for e in batch)

    def test_store_zone_mapping(self):
        """Test that zones are correctly mapped to stores."""
        simulator = EventSimulator()

        for _ in range(100):
            event = simulator.generate_event()
            assert event.zone_id in simulator.zones[event.store_id]
