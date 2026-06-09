using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Celeriant.Client;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Serialization;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;
using Celeriant.Reference;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

var celeriantAddress = builder.Configuration["Celeriant:Address"] ?? "localhost:10000";
var postgresConnStr = builder.Configuration.GetConnectionString("Postgres")
    ?? "Host=localhost;Database=celeriant_reference;Username=demo;Password=demo";

builder.Services.AddCeleriantPool(options =>
{
    options.Address = celeriantAddress;
});

builder.Services.AddSingleton(NpgsqlDataSource.Create(postgresConnStr));
builder.Services.AddSingleton<IdempotencyCache>();
builder.Services.AddSingleton<WatchBroadcaster>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<WatchBroadcaster>());
builder.Services.AddScoped<AccountService>();

var app = builder.Build();
app.UseStaticFiles();

var jsonOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
};

// Init Postgres schema + seed
await InitDatabase(app.Services);
await SeedAccounts(app.Services.GetRequiredService<ICeleriantPool>(),
    app.Services.GetRequiredService<NpgsqlDataSource>());

// ─────────────────── GET /api/accounts ───────────────────

app.MapGet("/api/accounts", () => Results.Json(new
{
    accounts = Constants.Accounts.Select(a => new { a.Id, a.Name }),
}, jsonOptions));

// ─────────────────── GET /api/accounts/{accountId}/balance ───────────────────

app.MapGet("/api/accounts/{accountId}/balance", async (
    Guid accountId,
    long? minBatchIndex,
    AccountService svc,
    CancellationToken ct) =>
{
    try
    {
        var projection = await svc.CatchUpAsync(accountId, minBatchIndex, ct);
        return Results.Json(new
        {
            balanceCents = projection.BalanceCents,
            batchIndex = projection.LastBatchIndex,
        }, jsonOptions);
    }
    catch (ConnectionFailedException)
    {
        return Results.Json(new { error = "SERVICE_UNAVAILABLE", message = "Celeriant server unreachable." },
            jsonOptions, statusCode: 503);
    }
});

// ─────────────────── GET /api/accounts/{accountId}/history ───────────────────

app.MapGet("/api/accounts/{accountId}/history", async (
    Guid accountId,
    long? fromBatchIndex,
    AccountService svc,
    CancellationToken ct) =>
{
    try
    {
        var (events, currentBatchIndex, balanceCents) = await svc.GetHistoryAsync(accountId, fromBatchIndex, ct);
        return Results.Json(new { events, currentBatchIndex, balanceCents }, jsonOptions);
    }
    catch (ConnectionFailedException)
    {
        return Results.Json(new { error = "SERVICE_UNAVAILABLE", message = "Celeriant server unreachable." },
            jsonOptions, statusCode: 503);
    }
});

// ─────────────────── POST /api/accounts/{accountId}/deposit ───────────────────

app.MapPost("/api/accounts/{accountId}/deposit", async (
    Guid accountId,
    AmountRequest req,
    HttpContext httpContext,
    AccountService svc,
    CancellationToken ct) =>
{
    var eventId = RequestEventId(httpContext);

    try
    {
        var result = await svc.DepositAsync(accountId, req.AmountCents, eventId, ct);
        return Results.Json(new { balanceCents = result.BalanceCents, batchIndex = result.BatchIndex }, jsonOptions);
    }
    catch (ValidationException ex)
    {
        return Results.Json(new { error = "VALIDATION_ERROR", message = ex.Message },
            jsonOptions, statusCode: 422);
    }
    catch (OccExhaustedException)
    {
        return Results.Json(new { error = "CONFLICT", message = "Account was modified concurrently. Please retry." },
            jsonOptions, statusCode: 409);
    }
    catch (ConnectionFailedException)
    {
        return Results.Json(new { error = "SERVICE_UNAVAILABLE", message = "Celeriant server unreachable." },
            jsonOptions, statusCode: 503);
    }
});

// ─────────────────── POST /api/accounts/{accountId}/withdraw ───────────────────

app.MapPost("/api/accounts/{accountId}/withdraw", async (
    Guid accountId,
    AmountRequest req,
    HttpContext httpContext,
    AccountService svc,
    CancellationToken ct) =>
{
    var eventId = RequestEventId(httpContext);

    try
    {
        var result = await svc.WithdrawAsync(accountId, req.AmountCents, eventId, ct);
        return Results.Json(new { balanceCents = result.BalanceCents, batchIndex = result.BatchIndex }, jsonOptions);
    }
    catch (InsufficientFundsException ex)
    {
        return Results.Json(new
        {
            error = "INSUFFICIENT_FUNDS",
            balanceCents = ex.BalanceCents,
            message = ex.Message,
        }, jsonOptions, statusCode: 422);
    }
    catch (ValidationException ex)
    {
        return Results.Json(new { error = "VALIDATION_ERROR", message = ex.Message },
            jsonOptions, statusCode: 422);
    }
    catch (OccExhaustedException)
    {
        return Results.Json(new { error = "CONFLICT", message = "Account was modified concurrently. Please retry." },
            jsonOptions, statusCode: 409);
    }
    catch (ConnectionFailedException)
    {
        return Results.Json(new { error = "SERVICE_UNAVAILABLE", message = "Celeriant server unreachable." },
            jsonOptions, statusCode: 503);
    }
});

// ─────────────────── POST /api/transfers ───────────────────

app.MapPost("/api/transfers", async (
    TransferRequest req,
    HttpContext httpContext,
    AccountService svc,
    CancellationToken ct) =>
{
    var eventId = RequestEventId(httpContext);

    try
    {
        var result = await svc.TransferAsync(req.FromAccountId, req.ToAccountId, req.AmountCents, eventId, ct);
        var response = new
        {
            from = new { balanceCents = result.From.BalanceCents, batchIndex = result.From.BatchIndex },
            to = new { balanceCents = result.To.BalanceCents, batchIndex = result.To.BatchIndex },
        };
        return Results.Json(response, jsonOptions);
    }
    catch (InsufficientFundsException ex)
    {
        return Results.Json(new
        {
            error = "INSUFFICIENT_FUNDS",
            balanceCents = ex.BalanceCents,
            message = ex.Message,
        }, jsonOptions, statusCode: 422);
    }
    catch (ValidationException ex)
    {
        return Results.Json(new { error = "VALIDATION_ERROR", message = ex.Message },
            jsonOptions, statusCode: 422);
    }
    catch (OccExhaustedException)
    {
        return Results.Json(new { error = "CONFLICT", message = "Accounts were modified concurrently. Please retry." },
            jsonOptions, statusCode: 409);
    }
    catch (ConnectionFailedException)
    {
        return Results.Json(new { error = "SERVICE_UNAVAILABLE", message = "Celeriant server unreachable." },
            jsonOptions, statusCode: 503);
    }
});

// ─────────────────── GET /api/watch/stream — SSE ───────────────────

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

// ─────────────────── Fallback to SPA ───────────────────

app.MapFallback(async context =>
{
    context.Response.ContentType = "text/html";
    await context.Response.SendFileAsync(
        Path.Combine(app.Environment.WebRootPath, "index.html"));
});

app.Run();

// ─────────────────── Idempotency helpers ───────────────────

// The Idempotency-Key header as a Guid, or a freshly minted one. It becomes the event_id
// on the write. A caller-supplied key makes HTTP retries resolvable without re-writing;
// any key, minted or not, is what lets an IdempotencyViolation be verified as ours rather
// than a sibling's.
static Guid RequestEventId(HttpContext context)
    => context.Request.Headers.TryGetValue("Idempotency-Key", out var header)
       && Guid.TryParse(header.ToString(), out var key)
        ? key
        : Guid.NewGuid();

// ─────────────────── Database init ───────────────────

async Task InitDatabase(IServiceProvider services)
{
    var db = services.GetRequiredService<NpgsqlDataSource>();
    await using var cmd = db.CreateCommand(@"
        CREATE TABLE IF NOT EXISTS account_balances (
            account_id                UUID PRIMARY KEY,
            account_name              TEXT NOT NULL,
            balance_cents             BIGINT NOT NULL DEFAULT 0,
            last_batch_index          BIGINT NOT NULL DEFAULT 0,
            last_client_event_index   BIGINT NOT NULL DEFAULT 0,
            updated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
        )");
    await cmd.ExecuteNonQueryAsync();
}

// ─────────────────── Seed ───────────────────

async Task SeedAccounts(ICeleriantPool pool, NpgsqlDataSource db)
{
    var serializer = JsonEventSerializer.Default;

    foreach (var (name, id, seedCents) in Constants.Accounts)
    {
        // Seed Postgres projection row
        await using (var cmd = db.CreateCommand(@"
            INSERT INTO account_balances (account_id, account_name, balance_cents, last_batch_index, last_client_event_index, updated_at)
            VALUES (@id, @name, 0, 0, 0, now())
            ON CONFLICT (account_id) DO NOTHING"))
        {
            cmd.Parameters.AddWithValue("id", id);
            cmd.Parameters.AddWithValue("name", name);
            await cmd.ExecuteNonQueryAsync();
        }

        // Seed Celeriant aggregate (if not already present)
        var key = Constants.AccountKey(id);
        try
        {
            var details = await pool.AggregateDetailsAsync(new AggregateDetailsRequest { AggregateKey = key });
            if (details.MaxAggregateVersion > 0)
                continue;
        }
        catch (AggregateNotFoundException)
        {
            // Doesn't exist yet — seed it
        }

        var evt = AggregateEventExtensions.Create(1L, new Deposited(seedCents), serializer);
        await pool.WriteAsync(key, [evt],
            clientId: Constants.ServiceClientId,
            allowCreate: true);

        Console.WriteLine($"Seeded {name} with ${seedCents / 100m:F2}");
    }
}

// ─────────────────── Request DTOs ───────────────────

record AmountRequest(int AmountCents);
record TransferRequest(Guid FromAccountId, Guid ToAccountId, int AmountCents);

// ─────────────────── Watch broadcaster ───────────────────

record WatchEvent(Guid AggregateId, string Operation, long? ToBatchIndex);

sealed class WatchBroadcaster(IConfiguration config, ILogger<WatchBroadcaster> logger) : BackgroundService
{
    private readonly ConcurrentDictionary<ChannelWriter<WatchEvent>, byte> _subscribers = new();

    public void Subscribe(ChannelWriter<WatchEvent> writer) => _subscribers.TryAdd(writer, 0);
    public void Unsubscribe(ChannelWriter<WatchEvent> writer) => _subscribers.TryRemove(writer, out _);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var address = config["Celeriant:Address"] ?? "localhost:10000";
        var accountIds = Constants.Accounts.Select(a => a.Id).ToHashSet();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var request = new WatchRequest
                {
                    Orgs = [Constants.OrgId],
                    AggregateTypes = [Constants.AccountTypeId],
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
