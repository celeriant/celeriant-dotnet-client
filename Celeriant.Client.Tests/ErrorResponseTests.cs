using Celeriant.Client.Responses;

namespace Celeriant.Client.Tests;

public class ErrorResponseTests
{
    // -----------------------------------------------------------------------
    // IsNotLeader
    // -----------------------------------------------------------------------

    [Theory]
    [InlineData(ErrorResponse.WriteNotLeader)]
    [InlineData(ErrorResponse.TrimNotLeader)]
    [InlineData(ErrorResponse.DeleteNotLeader)]
    public void IsNotLeader_NotLeaderErrorCodes_ReturnsTrue(uint errorCode)
    {
        var error = new ErrorResponse { ErrorCode = errorCode };
        Assert.True(error.IsNotLeader);
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(7001u)]
    [InlineData(ErrorResponse.IdentifyRequired)]
    public void IsNotLeader_OtherErrorCodes_ReturnsFalse(uint errorCode)
    {
        var error = new ErrorResponse { ErrorCode = errorCode };
        Assert.False(error.IsNotLeader);
    }

    // -----------------------------------------------------------------------
    // IsIdentityRequired
    // -----------------------------------------------------------------------

    [Fact]
    public void IsIdentityRequired_IdentifyRequiredCode_ReturnsTrue()
    {
        var error = new ErrorResponse { ErrorCode = ErrorResponse.IdentifyRequired };
        Assert.True(error.IsIdentityRequired);
    }

    [Fact]
    public void IsIdentityRequired_OtherCode_ReturnsFalse()
    {
        var error = new ErrorResponse { ErrorCode = 7001 };
        Assert.False(error.IsIdentityRequired);
    }

    // -----------------------------------------------------------------------
    // ParseLeaderAddress
    // -----------------------------------------------------------------------

    [Fact]
    public void ParseLeaderAddress_ValidJson_ReturnsAddress()
    {
        var error = new ErrorResponse
        {
            ErrorMessage = """{"leader_address":"new-leader:10000"}"""
        };
        Assert.Equal("new-leader:10000", error.ParseLeaderAddress());
    }

    [Fact]
    public void ParseLeaderAddress_JsonWithoutField_ReturnsNull()
    {
        var error = new ErrorResponse
        {
            ErrorMessage = """{"some_other_field":"value"}"""
        };
        Assert.Null(error.ParseLeaderAddress());
    }

    [Fact]
    public void ParseLeaderAddress_NotJson_ReturnsNull()
    {
        var error = new ErrorResponse { ErrorMessage = "plain text error" };
        Assert.Null(error.ParseLeaderAddress());
    }

    [Fact]
    public void ParseLeaderAddress_EmptyMessage_ReturnsNull()
    {
        var error = new ErrorResponse { ErrorMessage = "" };
        Assert.Null(error.ParseLeaderAddress());
    }

    [Fact]
    public void ParseLeaderAddress_NullLeaderValue_ReturnsNull()
    {
        var error = new ErrorResponse
        {
            ErrorMessage = """{"leader_address":null}"""
        };
        Assert.Null(error.ParseLeaderAddress());
    }

    // -----------------------------------------------------------------------
    // Well-known error code constants
    // -----------------------------------------------------------------------

    [Fact]
    public void ErrorCodeConstants_MatchExpectedValues()
    {
        Assert.Equal(2011u, ErrorResponse.WriteNotLeader);
        Assert.Equal(3005u, ErrorResponse.TrimNotLeader);
        Assert.Equal(4006u, ErrorResponse.DeleteNotLeader);
        Assert.Equal(10004u, ErrorResponse.IdentifyRequired);
        Assert.Equal(10005u, ErrorResponse.AuthRequired);
        Assert.Equal(10006u, ErrorResponse.AuthInvalidKey);
        Assert.Equal(10007u, ErrorResponse.AuthInsufficientPermissions);
    }
}
