using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Tests;

/// <summary>
/// Round-trip serialization tests for every request and response type
/// using WireCodec.Serialize / WireCodec.Deserialize.
/// </summary>
public class MessagePackSerializationTests
{
    private static readonly Guid OrgId            = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly Guid AggTypeId        = Guid.Parse("22222222-2222-2222-2222-222222222222");
    private static readonly Guid AggId            = Guid.Parse("33333333-3333-3333-3333-333333333333");
    private static readonly Guid ClientId         = Guid.Parse("44444444-4444-4444-4444-444444444444");
    private static readonly Guid UserId           = Guid.Parse("55555555-5555-5555-5555-555555555555");
    private static readonly Guid CorrelationId    = Guid.Parse("66666666-6666-6666-6666-666666666666");

    private static AggregateKey MakeKey() => new(OrgId, AggTypeId, AggId);

    // -----------------------------------------------------------------------
    // AggregateKey
    // -----------------------------------------------------------------------

    [Fact]
    public void AggregateKey_RoundTrip_AllFieldsPreserved()
    {
        var key = MakeKey();

        var bytes = WireCodec.Serialize(key);
        var result = WireCodec.Deserialize<AggregateKey>(bytes);

        Assert.Equal(OrgId, result.OrgId);
        Assert.Equal(AggTypeId, result.AggregateTypeId);
        Assert.Equal(AggId, result.AggregateId);
    }

    // -----------------------------------------------------------------------
    // ReadRequest
    // -----------------------------------------------------------------------

    [Fact]
    public void ReadRequest_RoundTrip_AllFieldsPreserved()
    {
        var req = new ReadRequest
        {
            CorrelationId = CorrelationId,
            AggregateKey  = MakeKey(),
            Filters       = new ReadFilters { FromAggregateVersion = 5, ToAggregateVersion = 100 },
        };

        var result = RoundTrip(req);

        Assert.Equal(CorrelationId, result.CorrelationId);
        Assert.Equal(OrgId, result.AggregateKey.OrgId);
        Assert.Equal(5L, result.Filters.FromAggregateVersion);
        Assert.Equal(100L, result.Filters.ToAggregateVersion);
    }

    [Fact]
    public void ReadRequest_NullCorrelationId_RoundTrip()
    {
        var req = new ReadRequest
        {
            CorrelationId = null,
            AggregateKey  = MakeKey(),
            Filters       = ReadFilters.From(1),
        };

        var result = RoundTrip(req);
        Assert.Null(result.CorrelationId);
    }

    // -----------------------------------------------------------------------
    // ReadFilters
    // -----------------------------------------------------------------------

    [Fact]
    public void ReadFilters_RoundTrip_AllFilters()
    {
        var filters = new ReadFilters
        {
            FromAggregateVersion = 10,
            ToAggregateVersion   = 50,
            IncludeEventTypes   = [1L, 2L, 3L],
            ExcludeClientId     = ClientId,
            IncludeClientId     = null,
            ExcludeUserId       = UserId,
            IncludeUserId       = null,
            MinServerTimestamp  = DateTimeOffset.FromUnixTimeMilliseconds(1000),
            MaxServerTimestamp  = DateTimeOffset.FromUnixTimeMilliseconds(9999),
            MinClientSeq = 0L,
            MaxClientSeq = 100L,
            MinEventTimestamp   = DateTimeOffset.FromUnixTimeMilliseconds(500),
            MaxEventTimestamp   = DateTimeOffset.FromUnixTimeMilliseconds(800),
            MinEventSeq       = 1L,
            MaxEventSeq       = 99L,
        };

        var result = RoundTrip(filters);

        Assert.Equal(10L, result.FromAggregateVersion);
        Assert.Equal(50L, result.ToAggregateVersion);
        Assert.Equal(new long[] { 1L, 2L, 3L }, result.IncludeEventTypes);
        Assert.Equal(ClientId, result.ExcludeClientId);
        Assert.Null(result.IncludeClientId);
        Assert.Equal(UserId, result.ExcludeUserId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1000), result.MinServerTimestamp);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(9999), result.MaxServerTimestamp);
        Assert.Equal(0L, result.MinClientSeq);
        Assert.Equal(100L, result.MaxClientSeq);
        Assert.Equal(1L, result.MinEventSeq);
        Assert.Equal(99L, result.MaxEventSeq);
    }

    [Fact]
    public void ReadFilters_NullableFields_NullByDefault()
    {
        var filters = ReadFilters.From(1);
        var result = RoundTrip(filters);

        Assert.Equal(1L, result.FromAggregateVersion);
        Assert.Null(result.ToAggregateVersion);
        Assert.Null(result.IncludeEventTypes);
        Assert.Null(result.ExcludeClientId);
        Assert.Null(result.IncludeClientId);
        Assert.Null(result.ExcludeUserId);
        Assert.Null(result.IncludeUserId);
        Assert.Null(result.MinServerTimestamp);
        Assert.Null(result.MaxServerTimestamp);
        Assert.Null(result.MinEventSeq);
        Assert.Null(result.MaxEventSeq);
    }

    // -----------------------------------------------------------------------
    // WriteRequest
    // -----------------------------------------------------------------------

    [Fact]
    public void WriteRequest_RoundTrip_AllFieldsPreserved()
    {
        var ev = new AggregateEvent
        {
            ClientSeq = 1,
            EventSeq       = 1,
            EventTimestamp   = DateTimeOffset.FromUnixTimeMilliseconds(12345),
            EventTypeMajor   = 10,
            EventTypeMinor   = 0,
            EventValue       = [0x01, 0x02, 0x03],
        };

        var write = new SingleAggregateWrite
        {
            Events       = [ev],
            AllowCreate  = true,
        };

        var req = new WriteRequest
        {
            CorrelationId = CorrelationId,
            ClientId      = ClientId,
            UserId        = UserId,
            Writes        = new Dictionary<AggregateKey, SingleAggregateWrite> { [MakeKey()] = write },
        };

        var result = RoundTrip(req);

        Assert.Equal(CorrelationId, result.CorrelationId);
        Assert.Equal(ClientId, result.ClientId);
        Assert.Equal(UserId, result.UserId);
        Assert.Single(result.Writes);
    }

    // -----------------------------------------------------------------------
    // DeleteRequest
    // -----------------------------------------------------------------------

    [Fact]
    public void DeleteRequest_RoundTrip()
    {
        var req = new DeleteRequest
        {
            CorrelationId = null,
            ClientId      = ClientId,
            UserId        = null,
            Deletes       = new Dictionary<AggregateKey, SingleAggregateDelete>
            {
                [MakeKey()] = new SingleAggregateDelete
                {
                    AllowRecreate           = false,
                    AllowSequenceContinuation  = true,
                    ExpectedVersion = 42L,
                }
            },
        };

        var result = RoundTrip(req);

        Assert.Null(result.CorrelationId);
        Assert.Equal(ClientId, result.ClientId);
        Assert.Single(result.Deletes);
        var del = result.Deletes.Values.First();
        Assert.False(del.AllowRecreate);
        Assert.True(del.AllowSequenceContinuation);
        Assert.Equal(42L, del.ExpectedVersion);
    }

    // -----------------------------------------------------------------------
    // TrimStartRequest
    // -----------------------------------------------------------------------

    [Fact]
    public void TrimStartRequest_RoundTrip()
    {
        var req = new TrimStartRequest
        {
            CorrelationId           = CorrelationId,
            AggregateKey            = MakeKey(),
            KeepFromAggregateVersion = 10,
            ClientId                = ClientId,
            UserId                  = null,
        };

        var result = RoundTrip(req);

        Assert.Equal(CorrelationId, result.CorrelationId);
        Assert.Equal(OrgId, result.AggregateKey.OrgId);
        Assert.Equal(10L, result.KeepFromAggregateVersion);
        Assert.Equal(ClientId, result.ClientId);
        Assert.Null(result.UserId);
    }

    // -----------------------------------------------------------------------
    // AggregateDetailsRequest
    // -----------------------------------------------------------------------

    [Fact]
    public void AggregateDetailsRequest_RoundTrip()
    {
        var req = new AggregateDetailsRequest
        {
            CorrelationId = CorrelationId,
            AggregateKey  = MakeKey(),
        };

        var result = RoundTrip(req);

        Assert.Equal(CorrelationId, result.CorrelationId);
        Assert.Equal(AggId, result.AggregateKey.AggregateId);
    }

    // -----------------------------------------------------------------------
    // ListOrgsRequest
    // -----------------------------------------------------------------------

    [Fact]
    public void ListOrgsRequest_RoundTrip()
    {
        var req = new ListOrgsRequest
        {
            CorrelationId = null,
            ShardId       = 3,
            Cursor        = 100,
        };

        var result = RoundTrip(req);

        Assert.Null(result.CorrelationId);
        Assert.Equal(3L, result.ShardId);
        Assert.Equal(100L, result.Cursor);
    }

    // -----------------------------------------------------------------------
    // ListAggregateTypesRequest
    // -----------------------------------------------------------------------

    [Fact]
    public void ListAggregateTypesRequest_RoundTrip()
    {
        var req = new ListAggregateTypesRequest
        {
            CorrelationId = CorrelationId,
            ShardId       = 1,
            OrgId         = OrgId,
            Cursor        = null,
        };

        var result = RoundTrip(req);

        Assert.Equal(CorrelationId, result.CorrelationId);
        Assert.Equal(1L, result.ShardId);
        Assert.Equal(OrgId, result.OrgId);
        Assert.Null(result.Cursor);
    }

    // -----------------------------------------------------------------------
    // ListAggregatesRequest
    // -----------------------------------------------------------------------

    [Fact]
    public void ListAggregatesRequest_RoundTrip()
    {
        var req = new ListAggregatesRequest
        {
            CorrelationId   = null,
            ShardId         = 2,
            OrgId           = OrgId,
            AggregateTypeId = AggTypeId,
            Cursor          = 50,
        };

        var result = RoundTrip(req);

        Assert.Null(result.CorrelationId);
        Assert.Equal(2L, result.ShardId);
        Assert.Equal(OrgId, result.OrgId);
        Assert.Equal(AggTypeId, result.AggregateTypeId);
        Assert.Equal(50L, result.Cursor);
    }

    // -----------------------------------------------------------------------
    // RegisterSchemaRequest
    // -----------------------------------------------------------------------

    [Fact]
    public void RegisterSchemaRequest_RoundTrip()
    {
        var req = new RegisterSchemaRequest
        {
            CorrelationId = CorrelationId,
            ClientId      = ClientId,
            UserId        = UserId,
            SchemaKey     = new SchemaKey
            {
                OrgId           = OrgId,
                AggregateTypeId = AggTypeId,
                EventTypeMajor  = 1,
                EventTypeMinor  = 0,
            },
            SchemaType = SchemaType.Avro,
            Schema     = "{\"type\":\"object\"}",
        };

        var result = RoundTrip(req);

        Assert.Equal(CorrelationId, result.CorrelationId);
        Assert.Equal(ClientId, result.ClientId);
        Assert.Equal(UserId, result.UserId);
        Assert.Equal(OrgId, result.SchemaKey.OrgId);
        Assert.Equal(1L, result.SchemaKey.EventTypeMajor);
        Assert.Equal(SchemaType.Avro, result.SchemaType);
        Assert.Equal("{\"type\":\"object\"}", result.Schema);
    }

    // -----------------------------------------------------------------------
    // IdentifyRequest
    // -----------------------------------------------------------------------

    [Fact]
    public void IdentifyRequest_RoundTrip()
    {
        var req = new IdentifyRequest
        {
            CorrelationId = null,
            PublicKey     = "dGVzdHB1YmxpY2tleQ==",
            Nonce         = "1234567890123",
            Signature     = "c2lnbmF0dXJl",
            ApiKey        = null,
        };

        var result = RoundTrip(req);

        Assert.Null(result.CorrelationId);
        Assert.Equal("dGVzdHB1YmxpY2tleQ==", result.PublicKey);
        Assert.Equal("1234567890123", result.Nonce);
        Assert.Equal("c2lnbmF0dXJl", result.Signature);
        Assert.Null(result.ApiKey);
    }

    // -----------------------------------------------------------------------
    // AggregateEvent: base64-encoded fields
    // -----------------------------------------------------------------------

    [Fact]
    public void AggregateEvent_RoundTrip_Base64Fields()
    {
        var eventId = Guid.Parse("77777777-7777-7777-7777-777777777777");
        var ev = new AggregateEvent
        {
            ClientSeq = 5,
            EventSeq       = 42,
            EventId          = eventId,
            EventTimestamp   = DateTimeOffset.FromUnixTimeMilliseconds(1700000000000),
            EventTypeMajor   = 3,
            EventTypeMinor   = 1,
            EventValue       = [0xDE, 0xAD, 0xBE, 0xEF],
            Iv               = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C],
        };

        var result = RoundTrip(ev);

        Assert.Equal(5L, result.ClientSeq);
        Assert.Equal(42L, result.EventSeq);
        Assert.Equal(eventId, result.EventId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1700000000000), result.EventTimestamp);
        Assert.Equal(3L, result.EventTypeMajor);
        Assert.Equal(1L, result.EventTypeMinor);
        Assert.Equal([0xDE, 0xAD, 0xBE, 0xEF], result.EventValue);
        Assert.Equal([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C], result.Iv);
    }

    [Fact]
    public void AggregateEvent_NullableFields_RoundTrip()
    {
        var ev = new AggregateEvent
        {
            ClientSeq = 1,
            EventSeq       = 1,
            EventId          = null,
            EventTimestamp   = DateTimeOffset.UnixEpoch,
            EventTypeMajor   = 0,
            EventTypeMinor   = 0,
            EventValue       = [],
            Iv               = null,
        };

        var result = RoundTrip(ev);

        Assert.Null(result.EventId);
        Assert.Null(result.Iv);
        Assert.Empty(result.EventValue);
    }

    // -----------------------------------------------------------------------
    // AggregateEventBatch: base64 Guid fields (ci, ui)
    // -----------------------------------------------------------------------

    [Fact]
    public void AggregateEventBatch_RoundTrip_Base64GuidFields()
    {
        var batch = new AggregateEventBatch
        {
            AggregateVersion = 7,
            ClientId        = ClientId,
            UserId          = UserId,
            ServerTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(1700000001000),
            Events          =
            [
                new AggregateEvent
                {
                    ClientSeq = 1,
                    EventSeq       = 1,
                    EventTimestamp   = DateTimeOffset.FromUnixTimeMilliseconds(100),
                    EventTypeMajor   = 1,
                    EventTypeMinor   = 0,
                    EventValue       = [0xFF],
                }
            ],
        };

        var result = RoundTrip(batch);

        Assert.Equal(7L, result.AggregateVersion);
        Assert.Equal(ClientId, result.ClientId);
        Assert.Equal(UserId, result.UserId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1700000001000), result.ServerTimestamp);
        Assert.Single(result.Events);
    }

    [Fact]
    public void AggregateEventBatch_NullUserId_RoundTrip()
    {
        var batch = new AggregateEventBatch
        {
            AggregateVersion = 1,
            ClientId        = ClientId,
            UserId          = null,
            ServerTimestamp = DateTimeOffset.UnixEpoch,
            Events          = [],
        };

        var result = RoundTrip(batch);

        Assert.Null(result.UserId);
        Assert.Equal(ClientId, result.ClientId);
        Assert.Empty(result.Events);
    }

    // -----------------------------------------------------------------------
    // ReadResponse
    // -----------------------------------------------------------------------

    [Fact]
    public void ReadResponse_RoundTrip()
    {
        var resp = new ReadResponse
        {
            CorrelationId       = CorrelationId,
            EventBatches        =
            [
                new AggregateEventBatch
                {
                    AggregateVersion = 1,
                    ClientId        = ClientId,
                    UserId          = null,
                    ServerTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(100),
                    Events          = [],
                }
            ],
            NextAggregateVersion = 2,
        };

        var result = RoundTrip(resp);

        Assert.Equal(CorrelationId, result.CorrelationId);
        Assert.Single(result.EventBatches);
        Assert.Equal(2L, result.NextAggregateVersion);
    }

    // -----------------------------------------------------------------------
    // SuccessResponse
    // -----------------------------------------------------------------------

    [Fact]
    public void SuccessResponse_RoundTrip()
    {
        var resp = new SuccessResponse { CorrelationId = CorrelationId };
        var result = RoundTrip(resp);
        Assert.Equal(CorrelationId, result.CorrelationId);
    }

    [Fact]
    public void SuccessResponse_NullCorrelationId_RoundTrip()
    {
        var resp = new SuccessResponse { CorrelationId = null };
        var result = RoundTrip(resp);
        Assert.Null(result.CorrelationId);
    }

    // -----------------------------------------------------------------------
    // WriteResponse
    // -----------------------------------------------------------------------

    [Fact]
    public void WriteResponse_RoundTrip()
    {
        var resp = new WriteResponse { CorrelationId = CorrelationId, MaxAggregateVersion = 42 };
        var result = RoundTrip(resp);
        Assert.Equal(CorrelationId, result.CorrelationId);
        Assert.Equal(42L, result.MaxAggregateVersion);
    }

    [Fact]
    public void WriteResponse_NullMaxAggregateVersion_RoundTrip()
    {
        var resp = new WriteResponse { CorrelationId = null, MaxAggregateVersion = null };
        var result = RoundTrip(resp);
        Assert.Null(result.CorrelationId);
        Assert.Null(result.MaxAggregateVersion);
    }

    // -----------------------------------------------------------------------
    // ErrorResponse
    // -----------------------------------------------------------------------

    [Fact]
    public void ErrorResponse_RoundTrip()
    {
        var resp = new ErrorResponse
        {
            CorrelationId = CorrelationId,
            ErrorCode     = 2011,
            ErrorMessage  = "not leader",
        };

        var result = RoundTrip(resp);

        Assert.Equal(CorrelationId, result.CorrelationId);
        Assert.Equal(2011u, result.ErrorCode);
        Assert.Equal("not leader", result.ErrorMessage);
    }

    // -----------------------------------------------------------------------
    // AggregateDetailsResponse
    // -----------------------------------------------------------------------

    [Fact]
    public void AggregateDetailsResponse_RoundTrip()
    {
        var resp = new AggregateDetailsResponse
        {
            CorrelationId           = CorrelationId,
            MinAggregateVersion      = 1,
            MaxAggregateVersion      = 100,
            MaxEventSeq           = 500,
            IsDeleted               = false,
            AllowRecreate           = true,
            AllowSequenceContinuation  = false,
            LastServerTimestamp     = DateTimeOffset.FromUnixTimeMilliseconds(9999),
            LastClientId            = ClientId,
            LastUserId              = UserId,
        };

        var result = RoundTrip(resp);

        Assert.Equal(CorrelationId, result.CorrelationId);
        Assert.Equal(1L, result.MinAggregateVersion);
        Assert.Equal(100L, result.MaxAggregateVersion);
        Assert.False(result.IsDeleted);
        Assert.True(result.AllowRecreate);
        Assert.Equal(ClientId, result.LastClientId);
        Assert.Equal(UserId, result.LastUserId);
    }

    // -----------------------------------------------------------------------
    // IdentifyResponse
    // -----------------------------------------------------------------------

    [Fact]
    public void IdentifyResponse_RoundTrip()
    {
        var resp = new IdentifyResponse
        {
            CorrelationId = CorrelationId,
            ClientId      = ClientId,
            AccessLevel   = AccessLevel.ReadWrite,
        };

        var result = RoundTrip(resp);

        Assert.Equal(CorrelationId, result.CorrelationId);
        Assert.Equal(ClientId, result.ClientId);
        Assert.Equal(AccessLevel.ReadWrite, result.AccessLevel);
    }

    // -----------------------------------------------------------------------
    // WatchResponse + WatchResponseEvent
    // -----------------------------------------------------------------------

    [Fact]
    public void WatchResponse_RoundTrip()
    {
        var resp = new WatchResponse
        {
            Events =
            [
                new WatchResponseEvent
                {
                    OrgId                   = OrgId,
                    AggregateTypeId         = AggTypeId,
                    AggregateId             = AggId,
                    Operation               = WatchOperationType.Write,
                    FromAggregateVersion     = 10,
                    ToAggregateVersion       = 20,
                    KeepFromAggregateVersion = null,
                }
            ]
        };

        var result = RoundTrip(resp);

        Assert.Single(result.Events);
        var ev = result.Events[0];
        Assert.Equal(OrgId, ev.OrgId);
        Assert.Equal(AggTypeId, ev.AggregateTypeId);
        Assert.Equal(AggId, ev.AggregateId);
        Assert.Equal(WatchOperationType.Write, ev.Operation);
        Assert.Equal(10L, ev.FromAggregateVersion);
        Assert.Equal(20L, ev.ToAggregateVersion);
        Assert.Null(ev.KeepFromAggregateVersion);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static T RoundTrip<T>(T value)
    {
        var bytes = WireCodec.Serialize(value);
        return WireCodec.Deserialize<T>(bytes);
    }
}
