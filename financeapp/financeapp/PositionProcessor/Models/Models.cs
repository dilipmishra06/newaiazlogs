namespace PositionProcessor.Models;

// Raw trade loaded from blob storage (CSV row)
public record TradeRecord(
    string TradeId,
    string Portfolio,
    string Instrument,
    decimal Quantity,
    decimal Price,
    string Direction,   // BUY or SELL
    DateTime TradeDate
);

// Calculated P&L position written to SQL
public record PositionResult(
    string Portfolio,
    string Instrument,
    decimal NetQuantity,
    decimal MarketValue,
    decimal PnL,
    DateTime CalculatedAt,
    string RunId
);

// Input/output for the orchestrator
public record ProcessingRequest(string BlobPath, string RunId, DateTime BusinessDate);
public record ProcessingResult(int TradesLoaded, int PositionsInserted, bool Success, string? Error);

// Input for InsertPositionsActivity — includes optional error message for AI reasoning
public record InsertInput(
    List<TradeRecord>? Trades,
    string RunId,
    string Status,
    string? ErrorMessage = null
);