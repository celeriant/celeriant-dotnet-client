using System.Text.Json;
using Celeriant.Client.Responses;
using Celeriant.Client.Serialization;

namespace Celeriant.Client.Tests;

public class EventSerializerTests
{
    private record OrderPlaced(int OrderId, decimal Total, string Customer);

    private record SensorReading(double[] Values, long TimestampMs);

    // -----------------------------------------------------------------------
    // JsonEventSerializer
    // -----------------------------------------------------------------------

    [Fact]
    public void JsonSerializer_RoundTrip_PreservesPayload()
    {
        var serializer = JsonEventSerializer.Default;
        var original = new OrderPlaced(42, 99.95m, "Alice");

        var bytes = serializer.Serialize(original);
        var result = serializer.Deserialize<OrderPlaced>(bytes);

        Assert.Equal(original, result);
    }

    [Fact]
    public void JsonSerializer_CustomOptions_Respected()
    {
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        var serializer = new JsonEventSerializer(options);
        var original = new OrderPlaced(1, 10m, "Bob");

        var bytes = serializer.Serialize(original);
        var json = System.Text.Encoding.UTF8.GetString(bytes);

        Assert.Contains("\"orderId\"", json);
        Assert.DoesNotContain("\"OrderId\"", json);

        var result = serializer.Deserialize<OrderPlaced>(bytes);
        Assert.Equal(original, result);
    }

    // -----------------------------------------------------------------------
    // AggregateEvent.Create<T>
    // -----------------------------------------------------------------------

    [Fact]
    public void Create_SetsEventValueFromSerializer()
    {
        var serializer = JsonEventSerializer.Default;
        var payload = new OrderPlaced(7, 49.99m, "Carol");

        var evt = AggregateEventExtensions.Create(
            eventTypeMajor: 1,
            payload,
            serializer);

        Assert.Equal(1, evt.EventTypeMajor);
        Assert.Equal(0, evt.EventTypeMinor);
        Assert.Equal(1, evt.ClientSeq);
        Assert.Equal(0, evt.EventSeq);

        var roundTripped = JsonSerializer.Deserialize<OrderPlaced>(evt.EventValue);
        Assert.Equal(payload, roundTripped);
    }

    [Fact]
    public void Create_AllParameters_Applied()
    {
        var serializer = JsonEventSerializer.Default;
        var eventId = Guid.NewGuid();
        var timestamp = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);

        var evt = AggregateEventExtensions.Create(
            eventTypeMajor: 10,
            new SensorReading([1.0, 2.0, 3.0], 1234567890),
            serializer,
            clientSeq: 5,
            eventTypeMinor: 3,
            eventId: eventId,
            timestamp: timestamp);

        Assert.Equal(10, evt.EventTypeMajor);
        Assert.Equal(3, evt.EventTypeMinor);
        Assert.Equal(5, evt.ClientSeq);
        Assert.Equal(eventId, evt.EventId);
        Assert.Equal(timestamp, evt.EventTimestamp);
    }

    // -----------------------------------------------------------------------
    // GetValue<T> extension
    // -----------------------------------------------------------------------

    [Fact]
    public void GetValue_DeserializesEventValue()
    {
        var serializer = JsonEventSerializer.Default;
        var original = new OrderPlaced(99, 250m, "Dave");

        var evt = AggregateEventExtensions.Create(
            eventTypeMajor: 1,
            original,
            serializer);

        var result = evt.GetValue<OrderPlaced>(serializer);
        Assert.Equal(original, result);
    }

    // -----------------------------------------------------------------------
    // Mixed serializers per event type
    // -----------------------------------------------------------------------

    [Fact]
    public void DifferentSerializers_PerEventType_WorkIndependently()
    {
        var json = JsonEventSerializer.Default;
        var camelJson = new JsonEventSerializer(
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

        var order = new OrderPlaced(1, 10m, "Eve");
        var sensor = new SensorReading([9.8, 9.7], 555);

        var evt1 = AggregateEventExtensions.Create(eventTypeMajor: 1, order, json, clientSeq: 1);
        var evt2 = AggregateEventExtensions.Create(eventTypeMajor: 2, sensor, camelJson, clientSeq: 2);

        // Each event deserializes with its own serializer
        Assert.Equal(order, evt1.GetValue<OrderPlaced>(json));

        var sensorResult = evt2.GetValue<SensorReading>(camelJson);
        Assert.Equal(sensor.TimestampMs, sensorResult.TimestampMs);
        Assert.Equal(sensor.Values, sensorResult.Values);
    }

    // -----------------------------------------------------------------------
    // Custom IEventSerializer
    // -----------------------------------------------------------------------

    /// <summary>
    /// A trivial custom serializer to prove the interface is implementable.
    /// Uses UTF-8 string round-trip for strings only.
    /// </summary>
    private sealed class Utf8StringSerializer : IEventSerializer
    {
        public byte[] Serialize<T>(T value)
            => System.Text.Encoding.UTF8.GetBytes(value?.ToString() ?? "");

        public T Deserialize<T>(ReadOnlySpan<byte> data)
            => (T)(object)System.Text.Encoding.UTF8.GetString(data);
    }

    [Fact]
    public void CustomSerializer_WorksWithCreateAndGetValue()
    {
        var serializer = new Utf8StringSerializer();

        var evt = AggregateEventExtensions.Create(
            eventTypeMajor: 42,
            "hello world",
            serializer);

        Assert.Equal("hello world"u8.ToArray(), evt.EventValue);
        Assert.Equal("hello world", evt.GetValue<string>(serializer));
    }
}
