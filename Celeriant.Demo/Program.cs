using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Celeriant.Client;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Serialization;
using Celeriant.Client.Watch;
using Celeriant.Demo.Domain;

var builder = WebApplication.CreateBuilder(args);

var celeriantAddress = builder.Configuration["Celeriant:Address"] ?? "localhost:10000";

builder.Services.AddCeleriantPool(options =>
{
    options.Address = celeriantAddress;
});

builder.Services.AddSingleton<WatchBroadcaster>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<WatchBroadcaster>());

var app = builder.Build();
app.UseStaticFiles();

var serializer = JsonEventSerializer.Default;
var jsonOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
};

// Seed accounts on startup
await SeedAccounts(app.Services.GetRequiredService<ICeleriantPool>());

// GET /api/accounts — metadata for frontend
app.MapGet("/api/accounts", () => Results.Json(new
{
    accounts = DemoConstants.Accounts.Select(a => new { a.Id, a.Name }),
    clients = DemoConstants.Clients.Select(c => new { c.Id, c.Name }),
}, jsonOptions));

// GET /api/accounts/{accountId}/events?fromVersion=1
app.MapGet("/api/accounts/{accountId}/events", async (
    Guid accountId,
    long? fromVersion,
    ICeleriantPool pool) =>
{
    var key = DemoConstants.AccountKey(accountId);
    var filters = ReadFilters.From(fromVersion ?? 1);

    try
    {
        var response = await pool.ReadAsync(new ReadRequest
        {
            AggregateKey = key,
            Filters = filters,
        });

        var batches = response.EventBatches.Select(b => new
        {
            version = b.AggregateVersion,
            clientId = b.ClientId,
            serverTimestamp = b.ServerTimestamp,
            events = b.Events.Select(e => DeserializeEvent(e)),
        });

        return Results.Json(new { batches }, jsonOptions);
    }
    catch (AggregateNotFoundException)
    {
        return Results.Json(new { batches = Array.Empty<object>() }, jsonOptions);
    }
});

// POST /api/accounts/{accountId}/deposit
app.MapPost("/api/accounts/{accountId}/deposit", async (
    Guid accountId,
    DepositRequest req,
    ICeleriantPool pool) =>
{
    var key = DemoConstants.AccountKey(accountId);
    var evt = AggregateEventExtensions.Create(1L, new Deposited(req.AmountCents), serializer);

    try
    {
        await pool.WriteAsync(key, [evt],
            clientId: req.ClientId,
            allowCreate: true,
            expectedVersion: req.ExpectedVersion);

        var details = await pool.AggregateDetailsAsync(new AggregateDetailsRequest { AggregateKey = key });
        return Results.Json(new { newVersion = details.MaxAggregateVersion }, jsonOptions);
    }
    catch (WriteOccException ex)
    {
        return Results.Json(new
        {
            error = "OCC_CONFLICT",
            currentVersion = ex.CurrentAggregateVersion,
            message = "Account was modified. Please refresh and retry.",
        }, jsonOptions, statusCode: 409);
    }
});

// POST /api/accounts/{accountId}/withdraw
app.MapPost("/api/accounts/{accountId}/withdraw", async (
    Guid accountId,
    WithdrawRequest req,
    ICeleriantPool pool) =>
{
    var key = DemoConstants.AccountKey(accountId);
    var evt = AggregateEventExtensions.Create(2L, new Withdrawn(req.AmountCents), serializer);

    try
    {
        await pool.WriteAsync(key, [evt],
            clientId: req.ClientId,
            allowCreate: true,
            expectedVersion: req.ExpectedVersion);

        var details = await pool.AggregateDetailsAsync(new AggregateDetailsRequest { AggregateKey = key });
        return Results.Json(new { newVersion = details.MaxAggregateVersion }, jsonOptions);
    }
    catch (WriteOccException ex)
    {
        return Results.Json(new
        {
            error = "OCC_CONFLICT",
            currentVersion = ex.CurrentAggregateVersion,
            message = "Account was modified. Please refresh and retry.",
        }, jsonOptions, statusCode: 409);
    }
});

// POST /api/transfers
app.MapPost("/api/transfers", async (TransferRequest req, ICeleriantPool pool) =>
{
    var fromKey = DemoConstants.AccountKey(req.FromAccountId);
    var toKey = DemoConstants.AccountKey(req.ToAccountId);

    var transferOutEvt = AggregateEventExtensions.Create(3L,
        new TransferredOut(req.AmountCents, req.ToAccountId), serializer);
    var transferInEvt = AggregateEventExtensions.Create(4L,
        new TransferredIn(req.AmountCents, req.FromAccountId), serializer);

    var writeRequest = new WriteRequest
    {
        ClientId = req.ClientId,
        Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
        {
            [fromKey] = new SingleAggregateWrite
            {
                Events = [transferOutEvt],
                AllowCreate = true,
                ExpectedVersion = req.ExpectedFromVersion,
            },
            [toKey] = new SingleAggregateWrite
            {
                Events = [transferInEvt],
                AllowCreate = true,
                ExpectedVersion = req.ExpectedToVersion,
            },
        },
    };

    try
    {
        await pool.WriteAsync(writeRequest);

        var fromDetails = await pool.AggregateDetailsAsync(new AggregateDetailsRequest { AggregateKey = fromKey });
        var toDetails = await pool.AggregateDetailsAsync(new AggregateDetailsRequest { AggregateKey = toKey });

        return Results.Json(new
        {
            newFromVersion = fromDetails.MaxAggregateVersion,
            newToVersion = toDetails.MaxAggregateVersion,
        }, jsonOptions);
    }
    catch (WriteOccException)
    {
        return Results.Json(new
        {
            error = "OCC_CONFLICT",
            message = "One or more accounts were modified. Please refresh and retry.",
        }, jsonOptions, statusCode: 409);
    }
});

// GET /api/watch/stream — SSE endpoint for live aggregate updates
app.MapGet("/api/watch/stream", async (HttpContext context, WatchBroadcaster broadcaster) =>
{
    context.Response.ContentType = "text/event-stream";
    context.Response.Headers.CacheControl = "no-cache";
    context.Response.Headers.Connection = "keep-alive";

    var channel = Channel.CreateBounded<WatchEvent>(new BoundedChannelOptions(64)
    {
        FullMode = BoundedChannelFullMode.DropOldest,
    });

    broadcaster.Subscribe(channel.Writer);
    try
    {
        await foreach (var evt in channel.Reader.ReadAllAsync(context.RequestAborted))
        {
            var data = JsonSerializer.Serialize(evt, jsonOptions);
            await context.Response.WriteAsync($"data: {data}\n\n", context.RequestAborted);
            await context.Response.Body.FlushAsync(context.RequestAborted);
        }
    }
    catch (OperationCanceledException) { }
    finally
    {
        broadcaster.Unsubscribe(channel.Writer);
    }
});

app.MapFallback(async context =>
{
    context.Response.ContentType = "text/html";
    await context.Response.SendFileAsync(
        Path.Combine(app.Environment.WebRootPath, "index.html"));
});

app.Run();

// --- Helper methods ---

object DeserializeEvent(AggregateEvent e)
{
    return e.EventTypeMajor switch
    {
        1 => ToDict(e.EventTypeMajor, serializer.Deserialize<Deposited>(e.EventValue)),
        2 => ToDict(e.EventTypeMajor, serializer.Deserialize<Withdrawn>(e.EventValue)),
        3 => ToDict(e.EventTypeMajor, serializer.Deserialize<TransferredOut>(e.EventValue)),
        4 => ToDict(e.EventTypeMajor, serializer.Deserialize<TransferredIn>(e.EventValue)),
        _ => new { eventTypeMajor = e.EventTypeMajor },
    };
}

object ToDict(long eventTypeMajor, object payload)
{
    var dict = new Dictionary<string, object> { ["eventTypeMajor"] = eventTypeMajor };
    foreach (var prop in payload.GetType().GetProperties())
    {
        var name = JsonNamingPolicy.CamelCase.ConvertName(prop.Name);
        var val = prop.GetValue(payload);
        if (val != null) dict[name] = val;
    }
    return dict;
}

async Task SeedAccounts(ICeleriantPool pool)
{
    foreach (var (name, id, seedCents) in DemoConstants.Accounts)
    {
        var key = DemoConstants.AccountKey(id);
        try
        {
            var details = await pool.AggregateDetailsAsync(new AggregateDetailsRequest { AggregateKey = key });
            if (details.MaxAggregateVersion > 0)
                continue; // Already has events
        }
        catch (AggregateNotFoundException)
        {
            // Doesn't exist yet — seed it
        }

        var evt = AggregateEventExtensions.Create(1L, new Deposited(seedCents), serializer);
        await pool.WriteAsync(key, [evt],
            clientId: DemoConstants.Clients[0].Id,
            allowCreate: true);

        Console.WriteLine($"Seeded {name} with ${seedCents / 100m:F2}");
    }
}

// --- Request DTOs ---

record DepositRequest(Guid ClientId, int AmountCents, long ExpectedVersion);
record WithdrawRequest(Guid ClientId, int AmountCents, long ExpectedVersion);
record TransferRequest(
    Guid ClientId,
    Guid FromAccountId,
    Guid ToAccountId,
    int AmountCents,
    long ExpectedFromVersion,
    long ExpectedToVersion);

// --- Watch broadcaster ---

record WatchEvent(Guid AggregateId, string Operation, long? ToVersion);

sealed class WatchBroadcaster(IConfiguration config, ILogger<WatchBroadcaster> logger) : BackgroundService
{
    private readonly ConcurrentDictionary<ChannelWriter<WatchEvent>, byte> _subscribers = new();

    public void Subscribe(ChannelWriter<WatchEvent> writer) => _subscribers.TryAdd(writer, 0);
    public void Unsubscribe(ChannelWriter<WatchEvent> writer) => _subscribers.TryRemove(writer, out _);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var address = config["Celeriant:Address"] ?? "localhost:10000";
        var accountIds = DemoConstants.Accounts.Select(a => a.Id).ToHashSet();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var request = new WatchRequest
                {
                    Orgs = [DemoConstants.OrgId],
                    AggregateTypes = [DemoConstants.AccountTypeId],
                    Aggregates = accountIds,
                    OperationTypes = [WatchOperationType.Write],
                };

                await using var connection = await WatchConnection.ConnectAsync(
                    address, request, new WatchOptions(), stoppingToken);

                logger.LogInformation("Watch connection established");

                while (!stoppingToken.IsCancellationRequested)
                {
                    var response = await connection.NextAsync(stoppingToken);
                    foreach (var evt in response.Events)
                    {
                        var watchEvent = new WatchEvent(
                            evt.AggregateId,
                            evt.Operation.ToString(),
                            evt.ToAggregateVersion);

                        foreach (var sub in _subscribers.Keys)
                        {
                            sub.TryWrite(watchEvent);
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Watch connection lost, reconnecting in 2s");
                await Task.Delay(2000, stoppingToken);
            }
        }
    }
}
