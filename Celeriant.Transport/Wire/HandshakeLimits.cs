namespace Celeriant.Transport;

/// <summary>
/// Protocol limits on the Identify handshake. The reply to Identify is read under these, not
/// under the caller's <c>MaxResponseSize</c>: the IdentifyResponse carries the compression
/// dictionary, which may be far larger than any data frame the caller expects.
/// Mirrors the Rust server's <c>celeriant_wal::builtin_dict::DICTIONARY_CEILING_BYTES</c> and
/// <c>celeriant_msg::process_identify::identify_response_max_bytes</c> for V5.
/// </summary>
public static class HandshakeLimits
{
    /// <summary>
    /// The largest compression dictionary a server may use, built-in or pre-staged. The server
    /// refuses to start with a larger one.
    /// </summary>
    public const int DictionaryCeilingBytes = 1_048_576;

    /// <summary>
    /// The largest IdentifyResponse body a V5 server can send. V5 encodes the dictionary with
    /// msgpack as an array of integers, so a byte of 0x80 or above takes two bytes on the wire
    /// (a uint8 marker plus the value): a ceiling-sized dictionary encodes to at most twice its
    /// length. The 128 bytes cover the rest of the envelope around it.
    /// </summary>
    public const int IdentifyResponseMaxBytes = 2 * DictionaryCeilingBytes + 128;

    /// <summary>
    /// The body bound for a reply to Identify, checked from its header before any body read.
    /// An IdentifyResponse may use the full handshake limit whatever the caller's cap, so a
    /// small cap still admits a large dictionary. Any other reply (an error frame) is held to the
    /// smaller of the caller's cap and that limit, so a hostile header can force neither a
    /// cap-sized nor an unbounded allocation.
    /// </summary>
    internal static long IdentifyReplyMaxBytes(bool isIdentifyResponse, long callerMaxResponseSize)
        => isIdentifyResponse
            ? IdentifyResponseMaxBytes
            : Math.Min(callerMaxResponseSize, IdentifyResponseMaxBytes);
}
