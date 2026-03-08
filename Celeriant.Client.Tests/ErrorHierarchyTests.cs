using Celeriant.Client.Errors;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Tests;

public class ErrorHierarchyTests
{
    // -----------------------------------------------------------------------
    // CeleriantClientException (base)
    // -----------------------------------------------------------------------

    [Fact]
    public void CeleriantClientException_MessageOnly_PreservesMessage()
    {
        var ex = new CeleriantClientException("test error");
        Assert.Equal("test error", ex.Message);
        Assert.Null(ex.InnerException);
    }

    [Fact]
    public void CeleriantClientException_WithInnerException_PreservesBoth()
    {
        var inner = new InvalidOperationException("inner");
        var ex = new CeleriantClientException("outer", inner);
        Assert.Equal("outer", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void CeleriantClientException_IsException()
    {
        Assert.IsAssignableFrom<Exception>(new CeleriantClientException());
    }

    // -----------------------------------------------------------------------
    // NotLeaderException
    // -----------------------------------------------------------------------

    [Fact]
    public void NotLeaderException_WithLeaderAddress_SetsProperties()
    {
        var error = new ErrorResponse { ErrorCode = ErrorResponse.WriteNotLeader, ErrorMessage = "{}" };
        var ex = new NotLeaderException(error, "leader:10000");

        Assert.Equal("leader:10000", ex.LeaderAddress);
        Assert.Same(error, ex.Error);
        Assert.Contains("leader:10000", ex.Message);
    }

    [Fact]
    public void NotLeaderException_NullLeaderAddress_MessageContainsUnknown()
    {
        var error = new ErrorResponse { ErrorCode = ErrorResponse.WriteNotLeader };
        var ex = new NotLeaderException(error, null);

        Assert.Null(ex.LeaderAddress);
        Assert.Contains("unknown", ex.Message);
    }

    [Fact]
    public void NotLeaderException_IsCeleriantClientException()
    {
        var error = new ErrorResponse { ErrorCode = ErrorResponse.WriteNotLeader };
        Assert.IsAssignableFrom<CeleriantClientException>(new NotLeaderException(error, null));
    }

    // -----------------------------------------------------------------------
    // IdentityRequiredException
    // -----------------------------------------------------------------------

    [Fact]
    public void IdentityRequiredException_PreservesError()
    {
        var error = new ErrorResponse { ErrorCode = ErrorResponse.IdentifyRequired };
        var ex = new IdentityRequiredException(error);

        Assert.Same(error, ex.Error);
        Assert.Contains("IdentifyAsync", ex.Message);
    }

    [Fact]
    public void IdentityRequiredException_IsCeleriantClientException()
    {
        var error = new ErrorResponse { ErrorCode = ErrorResponse.IdentifyRequired };
        Assert.IsAssignableFrom<CeleriantClientException>(new IdentityRequiredException(error));
    }

    // -----------------------------------------------------------------------
    // CeleriantErrorException
    // -----------------------------------------------------------------------

    [Fact]
    public void CeleriantErrorException_PreservesError()
    {
        var error = new ErrorResponse { ErrorCode = 7001, ErrorMessage = "aggregate not found" };
        var ex = new CeleriantErrorException(error);

        Assert.Same(error, ex.Error);
        Assert.Contains("7001", ex.Message);
        Assert.Contains("aggregate not found", ex.Message);
    }

    [Fact]
    public void CeleriantErrorException_IsCeleriantClientException()
    {
        var error = new ErrorResponse { ErrorCode = 1 };
        Assert.IsAssignableFrom<CeleriantClientException>(new CeleriantErrorException(error));
    }

    // -----------------------------------------------------------------------
    // ConnectionFailedException
    // -----------------------------------------------------------------------

    [Fact]
    public void ConnectionFailedException_MessageOnly_PreservesMessage()
    {
        var ex = new ConnectionFailedException("connection refused");
        Assert.Equal("connection refused", ex.Message);
        Assert.Null(ex.InnerException);
    }

    [Fact]
    public void ConnectionFailedException_WithInnerException_PreservesBoth()
    {
        var inner = new System.Net.Sockets.SocketException();
        var ex = new ConnectionFailedException("connection failed", inner);
        Assert.Equal("connection failed", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void ConnectionFailedException_IsCeleriantClientException()
    {
        Assert.IsAssignableFrom<CeleriantClientException>(new ConnectionFailedException("x"));
    }

    // -----------------------------------------------------------------------
    // ProtocolException
    // -----------------------------------------------------------------------

    [Fact]
    public void ProtocolException_MessageOnly_PreservesMessage()
    {
        var ex = new ProtocolException("bad message type");
        Assert.Equal("bad message type", ex.Message);
    }

    [Fact]
    public void ProtocolException_WithInnerException_PreservesBoth()
    {
        var inner = new FormatException("bad format");
        var ex = new ProtocolException("deserialization failed", inner);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void ProtocolException_IsCeleriantClientException()
    {
        Assert.IsAssignableFrom<CeleriantClientException>(new ProtocolException("x"));
    }

    // -----------------------------------------------------------------------
    // CeleriantTimeoutException
    // -----------------------------------------------------------------------

    [Fact]
    public void CeleriantTimeoutException_MessageOnly_PreservesMessage()
    {
        var ex = new CeleriantTimeoutException("request timed out");
        Assert.Equal("request timed out", ex.Message);
    }

    [Fact]
    public void CeleriantTimeoutException_WithInnerException_PreservesBoth()
    {
        var inner = new OperationCanceledException();
        var ex = new CeleriantTimeoutException("timed out", inner);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void CeleriantTimeoutException_IsCeleriantClientException()
    {
        Assert.IsAssignableFrom<CeleriantClientException>(new CeleriantTimeoutException("x"));
    }
}
