package batch1.app;


import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class CsvToDBStepConfiguration {


    private final JobRepository jobRepository;
    private final PlatformTransactionManager ptm;

    private final DataSource dataSource;

    private final Resource resource;

    public CsvToDBStepConfiguration(
            @Value("file:\\E:\\spring-learning\\batch-1\\data\\stock_fin.csv") Resource resource,
            JobRepository jobRepository, PlatformTransactionManager ptm, DataSource dataSource) {

        this.jobRepository = jobRepository;
        this.ptm = ptm;
        this.dataSource = dataSource;
        this.resource = resource;
    }

    static Double parseText(String text) {

        if (null == text || text.isBlank() || text.isEmpty() || text.contains("[a-z][A-Z]")) {
            return 0.0d;
        } else {
            return Double.valueOf(text);
        }
    }

    @Bean
    Step csvToDB() {
        return
                new StepBuilder("csvToDb", jobRepository)
                        .<StockDetails, StockDetails>chunk(100, ptm)
                        .reader(reader())
                        .writer(writer())
                        .build();
    }


    @Bean
    @StepScope
    FlatFileItemReader<StockDetails> reader() {


        return new FlatFileItemReaderBuilder<StockDetails>()
                .resource(resource)
                .name("compF_Csv_File")
                .delimited().delimiter(",")//
                .names("symbol,name,sector,price,earningsPerPrice,dividend,earningsPerShare,low52Week,high52Week,marketCap,EBITDA,pricePerSale,pricePerBook,secFilings"
                        .split(","))
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> new StockDetails(
                        fieldSet.readString(0),
                        fieldSet.readString(1),
                        fieldSet.readString(2),
                        parseText(fieldSet.readString(3)),
                        parseText(fieldSet.readString(4)),
                        parseText(fieldSet.readString(5)),
                        parseText(fieldSet.readString(6)),
                        parseText(fieldSet.readString(7)),
                        parseText(fieldSet.readString(8)),
                        fieldSet.readLong(9),
                        fieldSet.readLong(10),
                        parseText(fieldSet.readString(11)),
                        parseText(fieldSet.readString(12)),
                        fieldSet.readString(13)
                ))
                .build();
    }

    @Bean
    JdbcBatchItemWriter<StockDetails> writer() {
        String sql = """
                        insert into companyfinancial
                        (
                             symbol,
                             name,
                             sector,
                             price,
                             earningsperprice,
                             dividend,
                             earningspershare,
                             low52week,
                             high52week,
                             marketcap,
                             ebitda,
                             pricepersale,
                             priceperbook,
                             secfilings
                        )
                       values(
                             :symbol,
                             :name,
                             :sector,
                             :price,
                             :earningsperprice,
                             :dividend,
                             :earningspershare,
                             :low52week,
                             :high52week,
                             :marketcap,
                             :ebitda,
                             :pricepersale,
                             :priceperbook,
                             :secfilings
                       )
                       on conflict on constraint companyfinancial_symbol_name_sector_key do update set
                             symbol         =excluded.symbol,
                             name          =excluded.name,
                             sector         =excluded.sector,
                             price          =excluded.price,
                             earningsperprice =excluded.earningsperprice,
                             dividend       =excluded.dividend,
                             earningspershare =excluded.earningspershare,
                             low52week      =excluded.low52week,
                             high52week    =excluded.high52week,
                             marketcap      =excluded.marketcap,
                             ebitda       =excluded.ebitda,
                             pricepersale   =excluded.pricepersale,
                             priceperbook   =excluded.priceperbook,
                             secfilings      =excluded.secfilings
                """;
        return new JdbcBatchItemWriterBuilder<StockDetails>()
                .sql(sql)
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(item -> {
                    var map = new HashMap<String, Object>();
                    var map1 = Map
                            .of(
                                    "symbol", item.symbol(),
                                    "name", item.name(),
                                    "sector", item.sector(),
                                    "price", item.price(),
                                    "earningsperprice", item.earningsPerPrice(),
                                    "dividend", item.dividend(),
                                    "earningspershare", item.earningsPerPrice()

                            );
                    var map2 = Map.of(
                            "low52week", item.low52Week(),
                            "high52week", item.high52Week(),
                            "marketcap", item.marketCap(),
                            "ebitda", item.EBITDA(),
                            "pricepersale", item.pricePerSale(),
                            "priceperbook", item.pricePerBook(),
                            "secfilings", item.secFilings()

                    );

                    map.putAll(map1);
                    map.putAll(map2);

                    return new MapSqlParameterSource(map);
                })
                .itemPreparedStatementSetter(
                        (StockDetails item, PreparedStatement ps) -> {
                            ps.setString(0, item.symbol());
                            ps.setString(1, item.name());
                            ps.setString(2, item.sector());
                            ps.setDouble(3, item.price());
                            ps.setDouble(4, item.earningsPerPrice());
                            ps.setDouble(5, item.dividend());
                            ps.setDouble(6, item.earningsPerShare());
                            ps.setDouble(7, item.low52Week());
                            ps.setDouble(8, item.high52Week());
                            ps.setLong(9, item.marketCap());
                            ps.setLong(10, item.EBITDA());
                            ps.setDouble(11, item.pricePerSale());
                            ps.setDouble(12, item.pricePerBook());
                            ps.setString(13, item.secFilings());
                            ps.execute();
                        })
                .build();
    }

}
