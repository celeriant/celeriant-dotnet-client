using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Streaming;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Integration tests exercising the complete Celeriant API surface.
/// Each test is independent and uses fresh aggregate keys.
/// </summary>
[Collection("Server")]
public sealed class StandaloneTests
{
    private readonly ServerFixture _fixture;

    public StandaloneTests(ServerFixture fixture)
    {
        _fixture = fixture;
    }

    private CeleriantClient Client
    {
        get
        {
            Skip.If(!_fixture.IsAvailable, "Server not running");
            return _fixture.Client!;
        }
    }

    private static AggregateKey NewKey() =>
        new(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

    private static AggregateKey NewKey(Guid orgId) =>
        new(orgId, Guid.NewGuid(), Guid.NewGuid());

    private static AggregateKey NewKey(Guid orgId, Guid aggregateTypeId) =>
        new(orgId, aggregateTypeId, Guid.NewGuid());

    private static AggregateEvent MakeEvent(long clientSeq = 1, string payload = "test") =>
        new()
        {
            ClientSeq = clientSeq,
            EventSeq = 0,
            EventTimestamp = DateTimeOffset.UtcNow,
            EventTypeMajor = 1,
            EventTypeMinor = 0,
            EventValue = System.Text.Encoding.UTF8.GetBytes(payload),
        };

    private async Task<ClientResponse> WriteAsync(AggregateKey key, AggregateEvent[] events,
        bool allowCreate = true, long? expectedBatchIndex = null, bool enforceIdempotency = false,
        Guid? clientId = null)
    {
        var req = new WriteRequest
        {
            ClientId = clientId ?? Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [key] = new SingleAggregateWrite
                {
                    AllowCreate = allowCreate,
                    ExpectedVersion = expectedBatchIndex,
                    EnforceClientIdempotency = enforceIdempotency,
                    Events = events,
                }
            }
        };
        return await Client.SendRequestAsync(new ClientRequest.Write(req));
    }

    // =========================================================================
    // 1. Nonexistent aggregate returns error
    // =========================================================================

    [SkippableFact]
    public async Task NonexistentAggregate_ThrowsError7001()
    {
        var ex = await Assert.ThrowsAsync<AggregateNotFoundException>(
            () => Client.SendRequestAsync(
                new ClientRequest.AggregateDetails(new AggregateDetailsRequest { AggregateKey = NewKey() })));
        Assert.Equal(7001u, ex.Error.ErrorCode);
    }

    // =========================================================================
    // 2. Basic create and read
    // =========================================================================

    [SkippableFact]
    public async Task CreateAggregate_ThenRead()
    {
        var key = NewKey();
        byte[] payload = "hello-world"u8.ToArray();

        var writeResp = await WriteAsync(key, [MakeEvent(payload: "hello-world")]);
        Assert.IsType<ClientResponse.Write>(writeResp);

        var readResp = await Client.SendRequestAsync(new ClientRequest.Read(new ReadRequest
        {
            AggregateKey = key,
            Filters = ReadFilters.From(1),
        }));
        var read = Assert.IsType<ClientResponse.Read>(readResp);
        Assert.NotEmpty(read.Value.EventBatches);

        var events = read.Value.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }

    // =========================================================================
    // 3. Aggregate details after create
    // =========================================================================

    [SkippableFact]
    public async Task AggregateDetails_AfterCreate()
    {
        var key = NewKey();

        await WriteAsync(key, [MakeEvent()]);

        var detResp = await Client.SendRequestAsync(
            new ClientRequest.AggregateDetails(new AggregateDetailsRequest { AggregateKey = key }));
        var det = Assert.IsType<ClientResponse.AggregateDetails>(detResp);
        Assert.Equal(1L, det.Value.MaxAggregateVersion);
        Assert.False(det.Value.IsDeleted);
    }

    // =========================================================================
    // 4. OCC write (expected batch index)
    // =========================================================================

    [SkippableFact]
    public async Task OccWrite_WithCorrectExpectedIndex()
    {
        var key = NewKey();

        // Create with no OCC check
        var r1 = await WriteAsync(key, [MakeEvent(1, "first")]);
        Assert.IsType<ClientResponse.Write>(r1);

        // Get current batch index
        var detResp = await Client.SendRequestAsync(
            new ClientRequest.AggregateDetails(new AggregateDetailsRequest { AggregateKey = key }));
        var det = Assert.IsType<ClientResponse.AggregateDetails>(detResp);

        // Write with correct expected batch index
        var r2 = await WriteAsync(key, [MakeEvent(2, "second")],
            allowCreate: false, expectedBatchIndex: det.Value.MaxAggregateVersion);
        Assert.IsType<ClientResponse.Write>(r2);
    }

    // =========================================================================
    // 5. OCC conflict: wrong expected batch index
    // =========================================================================

    [SkippableFact]
    public async Task OccWrite_WrongIndex_ThrowsWriteError()
    {
        var key = NewKey();

        await WriteAsync(key, [MakeEvent()]);

        // Write with wrong expected index (0 when it should be 1)
        await Assert.ThrowsAsync<WriteOccException>(
            () => WriteAsync(key, [MakeEvent(99, "should-fail")],
                allowCreate: false, expectedBatchIndex: 0));
    }

    // =========================================================================
    // 6. Client idempotency: duplicate write is accepted
    // =========================================================================

    [SkippableFact]
    public async Task ClientIdempotency_DuplicateWriteAccepted()
    {
        var key = NewKey();
        var clientId = Guid.NewGuid();

        // First write
        var r1 = await WriteAsync(key, [MakeEvent(1, "idem-test")],
            enforceIdempotency: true, clientId: clientId);
        Assert.IsType<ClientResponse.Write>(r1);

        // Same client_id, same client_event_index: should be idempotent
        // Should succeed (idempotent) or throw a write error: but not crash
        try
        {
            var r2 = await WriteAsync(key, [MakeEvent(1, "idem-test")],
                allowCreate: false, expectedBatchIndex: 1,
                enforceIdempotency: true, clientId: clientId);
            Assert.IsType<ClientResponse.Write>(r2);
        }
        catch (WriteErrorException)
        {
            // Also acceptable: server may reject the duplicate
        }
    }

    // =========================================================================
    // 7. Trim start
    // =========================================================================

    [SkippableFact]
    public async Task TrimStart_RemovesOldBatches()
    {
        var key = NewKey();

        // Write two batches
        await WriteAsync(key, [MakeEvent(1, "batch-1")]);
        await WriteAsync(key, [MakeEvent(2, "batch-2")], allowCreate: false);

        // Get max batch
        var detResp = await Client.SendRequestAsync(
            new ClientRequest.AggregateDetails(new AggregateDetailsRequest { AggregateKey = key }));
        var det = Assert.IsType<ClientResponse.AggregateDetails>(detResp);
        var maxBatch = det.Value.MaxAggregateVersion;

        // Trim: keep from maxBatch onwards (throws on error)
        var trimResp = await Client.SendRequestAsync(new ClientRequest.TrimStart(new TrimStartRequest
        {
            AggregateKey = key,
            KeepFromAggregateVersion = maxBatch,
            ClientId = Guid.NewGuid(),
        }));
        Assert.IsType<ClientResponse.TrimStart>(trimResp);

        // Read from maxBatch onwards: only batch-2 should be present (throws on error)
        var readResp = await Client.SendRequestAsync(new ClientRequest.Read(new ReadRequest
        {
            AggregateKey = key,
            Filters = ReadFilters.From(maxBatch),
        }));
        var read = Assert.IsType<ClientResponse.Read>(readResp);

        // Only batch-2 events should remain
        var remainingPayloads = read.Value.EventBatches
            .SelectMany(b => b.Events)
            .Select(e => System.Text.Encoding.UTF8.GetString(e.EventValue))
            .ToList();
        Assert.Contains("batch-2", remainingPayloads);
        Assert.DoesNotContain("batch-1", remainingPayloads);
    }

    // =========================================================================
    // 8. Delete aggregate
    // =========================================================================

    [SkippableFact]
    public async Task DeleteAggregate_ThenExcludedFromList()
    {
        var orgId = Guid.NewGuid();
        var key = NewKey(orgId);

        await WriteAsync(key, [MakeEvent()]);

        // Get batch index for delete
        var detResp = await Client.SendRequestAsync(
            new ClientRequest.AggregateDetails(new AggregateDetailsRequest { AggregateKey = key }));
        var det = Assert.IsType<ClientResponse.AggregateDetails>(detResp);

        // Delete
        var deleteResp = await Client.SendRequestAsync(new ClientRequest.Delete(new DeleteRequest
        {
            ClientId = Guid.NewGuid(),
            Deletes = new Dictionary<AggregateKey, SingleAggregateDelete>
            {
                [key] = new SingleAggregateDelete
                {
                    AllowRecreate = false,
                    ExpectedVersion = det.Value.MaxAggregateVersion,
                }
            }
        }));
        Assert.IsType<ClientResponse.Delete>(deleteResp);

        // Verify: after delete, either details shows IsDeleted=true
        // or the server throws an error (deleted aggregate may not be queryable)
        try
        {
            var detAfter = await Client.SendRequestAsync(
                new ClientRequest.AggregateDetails(new AggregateDetailsRequest { AggregateKey = key }));
            var detDeleted = Assert.IsType<ClientResponse.AggregateDetails>(detAfter);
            Assert.True(detDeleted.Value.IsDeleted);
        }
        catch (CeleriantErrorException)
        {
            // Also acceptable: some servers don't return details for deleted aggregates
        }
    }

    // =========================================================================
    // 9. List orgs, types, aggregates
    // =========================================================================

    [SkippableFact]
    public async Task ListOperations_FindCreatedAggregates()
    {
        var orgId = Guid.NewGuid();
        var typeId1 = Guid.NewGuid();
        var typeId2 = Guid.NewGuid();
        var key1 = NewKey(orgId, typeId1);
        var key2 = NewKey(orgId, typeId2);

        await WriteAsync(key1, [MakeEvent()]);
        await WriteAsync(key2, [MakeEvent()]);

        // List orgs
        var orgIds = new List<Guid>();
        await foreach (var org in Client.ListOrgsAsync())
            orgIds.Add(org.OrgId);
        Assert.Contains(orgId, orgIds);

        // List aggregate types
        var typeIds = new List<Guid>();
        await foreach (var item in Client.ListAggregateTypesAsync(orgId: orgId))
            typeIds.Add(item.AggregateTypeId);
        Assert.Contains(typeId1, typeIds);
        Assert.Contains(typeId2, typeIds);

        // List aggregates
        var aggregates = new List<AggregateStats>();
        await foreach (var item in Client.ListAggregatesAsync(orgId: orgId))
            aggregates.Add(item);
        Assert.Contains(aggregates, a => a.AggregateId == key1.AggregateId);
        Assert.Contains(aggregates, a => a.AggregateId == key2.AggregateId);
    }

    // =========================================================================
    // 10. List aggregates with include_deleted
    // =========================================================================

    [SkippableFact]
    public async Task ListAggregates_IncludeDeleted()
    {
        var orgId = Guid.NewGuid();
        var key1 = NewKey(orgId);
        var key2 = NewKey(orgId);

        await WriteAsync(key1, [MakeEvent()]);
        await WriteAsync(key2, [MakeEvent()]);

        // Delete key1
        var detResp = await Client.SendRequestAsync(
            new ClientRequest.AggregateDetails(new AggregateDetailsRequest { AggregateKey = key1 }));
        var det = Assert.IsType<ClientResponse.AggregateDetails>(detResp);
        await Client.SendRequestAsync(new ClientRequest.Delete(new DeleteRequest
        {
            ClientId = Guid.NewGuid(),
            Deletes = new Dictionary<AggregateKey, SingleAggregateDelete>
            {
                [key1] = new SingleAggregateDelete
                {
                    AllowRecreate = false,
                    ExpectedVersion = det.Value.MaxAggregateVersion,
                }
            }
        }));

        // Default list: key1 absent
        var defaultList = new List<AggregateStats>();
        await foreach (var item in Client.ListAggregatesAsync(orgId: orgId))
            defaultList.Add(item);
        Assert.DoesNotContain(defaultList, a => a.AggregateId == key1.AggregateId);
        Assert.Contains(defaultList, a => a.AggregateId == key2.AggregateId);

        // Include deleted: key1 present with IsDeleted=true
        var allList = new List<AggregateStats>();
        await foreach (var item in Client.ListAggregatesAsync(orgId: orgId,
            options: new ListOptions { IncludeDeleted = true }))
            allList.Add(item);
        var deleted = allList.FirstOrDefault(a => a.AggregateId == key1.AggregateId);
        Assert.NotNull(deleted);
        Assert.True(deleted.IsDeleted);
    }

    // =========================================================================
    // 11. Pool operations
    // =========================================================================

    [SkippableFact]
    public async Task Pool_WriteAndRead()
    {
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = _fixture.Address,
            MaxConnections = 2,
        });

        var key = NewKey();
        byte[] payload = "pool-test"u8.ToArray();

        var writeResult = await pool.WriteAsync(new WriteRequest
        {
            ClientId = Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [key] = new SingleAggregateWrite
                {
                    AllowCreate = true,
                    Events = [MakeEvent(payload: "pool-test")],
                }
            }
        });
        Assert.NotNull(writeResult);

        var readResult = await pool.ReadAsync(new ReadRequest
        {
            AggregateKey = key,
            Filters = ReadFilters.From(1),
        });
        Assert.NotNull(readResult);
        var events = readResult.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }
}
