package batch1.app;

public record StockDetails(
            String symbol,
            String name,
            String sector,
            Double price,
            Double earningsPerPrice,
            Double dividend,
            Double earningsPerShare,
            Double low52Week,
            Double high52Week,
            Long marketCap,
            Long EBITDA,
            Double pricePerSale,
            Double pricePerBook,
            String secFilings) {
    }
