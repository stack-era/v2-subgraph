import psycopg2


conn = psycopg2.connect(
    host="localhost",
    database="graph-node",
    user="graph-node",
    password="let-me-in"
)

with conn:
    with conn.cursor() as cur:
        while True:
            # Start transactions
            q = "SELECT name FROM public.deployment_schemas ORDER BY created_at DESC LIMIT 1;"
            cur.execute(q)
            subgraph_name = cur.fetchone()
            print("SUBGRAPH: {}".format(subgraph_name))
            if subgraph_name is None:
                continue
            # CREATE table to store pair_history
            q = """
                CREATE TABLE IF NOT EXISTS public.pair_history (
                    pair text NULL,
                    "timestamp" timestamp with time zone NULL,
                    "usdPrice" numeric NULL,
                    "ethPrice" numeric NULL,
                    volume numeric NULL,
                    id SERIAL
                );
                """
            cur.execute(q)
            # Create Extension
            q = "CREATE EXTENSION IF NOT EXISTS timescaledb;"
            cur.execute(q)
            # Create hypertable
            q = "SELECT create_hypertable('public.pair_history', 'timestamp');"
            cur.execute(q)
            # Create function to process swaps data into pair_history
            q = """
                CREATE OR REPLACE FUNCTION trades_copy() RETURNS TRIGGER AS
                $BODY$
                DECLARE
                pair text := new.pair;
                timestamp_ timestamptz := to_timestamp(new.timestamp);
                usd_price numeric;
                eth_price_var numeric;
                volume numeric := 0;
                token_0_var text;
                token_1_var text;
                amount_in numeric := 0;
                amount_out numeric := 0;
                amount_usd numeric := 0;
                token_quantity numeric := 0;
                token_0_price numeric := 0;
                token_1_price numeric := 0;
                price numeric := 0;
                price_usd numeric := 0;
                total numeric := 0;
                BEGIN
                    SELECT token_0, token_1
                    INTO token_0_var, token_1_var
                    FROM {}.pair
                    WHERE {}.pair.id = new.pair
                    LIMIT 1;
                    IF (token_0_var = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' or (token_0_var != '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' and token_1_var = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')) THEN
                        IF (token_1_var = '0xdac17f958d2ee523a2206206994597c13d831ec7' or token_1_var = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') THEN
                            SELECT eth_price
                            INTO eth_price_var
                            FROM {}.bundle
                            WHERE {}.bundle.block_range @> lower(new.block_range);

                            IF new.amount_0_in != 0 THEN
                                amount_out := ROUND(new.amount_1_out, 18);
                                amount_usd := ROUND(new.amount_usd, 18);
                                token_quantity := ROUND(new.amount_0_in, 18);

                                IF (token_quantity != 0 and amount_out != 0) THEN
                                    token_0_price := amount_usd / token_quantity;
                                    token_1_price := amount_usd / amount_out;
                                    IF token_0_price != 0 THEN
                                        price_usd := ROUND((token_0_price / token_1_price), 18);
                                    END IF;
                                    IF eth_price_var != 0 THEN
                                        price := price_usd / eth_price_var;
                                    END IF;
                                    total := price * token_quantity;
                                END IF;
                            ELSE
                                amount_in := ROUND(new.amount_1_in, 18);
                                amount_usd := ROUND(new.amount_usd, 18);
                                token_quantity := ROUND(new.amount_0_out, 18);

                                IF (token_quantity != 0 and amount_in != 0) THEN
                                    token_0_price := amount_usd / token_quantity;
                                    token_1_price := amount_usd / amount_in;
                                    IF token_1_price != 0 THEN
                                        price_usd := ROUND((token_0_price / token_1_price), 18);
                                    END IF;
                                    IF eth_price_var != 0 THEN
                                        price := price_usd / eth_price_var;
                                    END IF;
                                    total := price * token_quantity;
                                END IF;
                            END IF;
                        ELSE
                            IF new.amount_1_in != 0 THEN
                                amount_in := ROUND(new.amount_0_out, 18);
                                amount_usd := ROUND(new.amount_usd, 18);
                                token_quantity := ROUND(new.amount_1_in, 18);
                                IF token_quantity != 0 THEN
                                    price_usd := ROUND((amount_usd / token_quantity), 18);
                                END IF;
                                total := ROUND(amount_in, 18);

                                IF (token_quantity != 0 and amount_in != 0) THEN
                                    token_0_price := amount_usd / token_quantity;
                                    token_1_price := amount_usd / amount_in;
                                END IF;

                                IF token_1_price != 0 THEN
                                    price = ROUND((token_0_price / token_1_price), 18);
                                END IF;
                            ELSE
                                amount_out := ROUND(new.amount_0_in, 18);
                                amount_usd := ROUND(new.amount_usd, 18);
                                token_quantity := ROUND(new.amount_1_out, 18);
                                IF token_quantity != 0 THEN
                                    price_usd := ROUND((amount_usd / token_quantity), 18);
                                END IF;
                                total := ROUND(amount_out, 18);
                                IF (token_quantity != 0 and amount_out != 0) THEN
                                    token_0_price := amount_usd / token_quantity;
                                    token_1_price := amount_usd / amount_out;
                                END IF;
                                IF token_1_price != 0 THEN
                                    price := ROUND((token_0_price / token_1_price), 18);
                                END IF;
                            END IF;
                        END IF;
                    ELSE
                        IF new.amount_0_in != 0 THEN
                            amount_out := ROUND(new.amount_1_out, 18);
                            amount_usd := ROUND(new.amount_usd, 18);
                            token_quantity := ROUND(new.amount_0_in, 18);
                            IF token_quantity != 0 THEN
                                price_usd = ROUND((amount_usd / token_quantity), 18);
                            END IF;
                            total = ROUND(amount_out, 18);
                            IF (token_quantity != 0 and amount_out != 0) THEN
                                token_0_price := amount_usd / token_quantity;
                                token_1_price := amount_usd / amount_out;
                                IF token_1_price != 0 THEN
                                    price := ROUND((token_0_price / token_1_price), 18);
                                END IF;
                            END IF;
                        ELSE
                            amount_in := ROUND(new.amount_1_in, 18);
                            amount_usd := ROUND(new.amount_usd, 18);
                            token_quantity := ROUND(new.amount_0_out, 18);
                            IF token_quantity != 0 THEN
                                price_usd := ROUND((amount_usd / token_quantity), 18);
                            END IF;
                            total := ROUND(amount_in, 18);
                            IF (token_quantity != 0 and amount_in != 0) THEN
                                token_0_price = amount_usd / token_quantity;
                                token_1_price = amount_usd / amount_in;
                                IF token_1_price != 0 THEN
                                    price = ROUND((token_0_price / token_1_price), 18);
                                END IF;
                            END IF;
                        END IF;
                    END IF;

                    INSERT INTO public.pair_history (pair, timestamp, "usdPrice", "ethPrice", volume)
                    VALUES (new.pair, timestamp_, price_usd, price, total);
                    
                    RETURN new;
                END;
                $BODY$
                language plpgsql;
                """.format(subgraph_name[0], subgraph_name[0], subgraph_name[0], subgraph_name[0])
            cur.execute(q)
            # Create trigger
            q = """
                CREATE TRIGGER swap_process
                    AFTER INSERT ON {}.swap
                    FOR EACH ROW
                    EXECUTE PROCEDURE trades_copy();
                """.format(subgraph_name[0])
            cur.execute(q)
        
            break