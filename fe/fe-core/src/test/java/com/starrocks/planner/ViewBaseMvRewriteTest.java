package com.starrocks.planner;

import com.starrocks.common.Config;
import org.junit.BeforeClass;
import org.junit.Test;

public class ViewBaseMvRewriteTest extends MaterializedViewTestBase{
    @BeforeClass
    public static void setUp() throws Exception {
        MaterializedViewTestBase.setUp();

        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);
        Config.default_replication_num = 1;
    }

    @Test
    public void testViewBasedMvRewrite() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(30000000);
        /*{
            starRocksAssert.withView("create view agg_view_1" +
                    " as " +
                    " select c1, sum(c2) as total from t1 group by c1");

            {
                String mv = "select c5, c6, c1, total from t2 join agg_view_1 on c5 = c1";
                String query = "select c5, c6, c1, total from t2 join agg_view_1 on c5 = c1";
                testRewriteOK(mv, query);
            }
        }*/
        {
            String view = "create view view_q1\n" +
                    "as\n" +
                    "select\n" +
                    "    l_returnflag,\n" +
                    "    l_linestatus,\n" +
                    "    l_shipdate,\n" +
                    "    sum(l_quantity) as sum_qty,\n" +
                    "    sum(l_extendedprice) as sum_base_price,\n" +
                    "    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n" +
                    "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n" +
                    "    avg(l_quantity) as avg_qty,\n" +
                    "    avg(l_extendedprice) as avg_price,\n" +
                    "    avg(l_discount) as avg_disc,\n" +
                    "    count(*) as count_order\n" +
                    "from\n" +
                    "    test.lineitem\n" +
                    "group by\n" +
                    "    l_returnflag,\n" +
                    "    l_linestatus,\n" +
                    "    l_shipdate";
            starRocksAssert.withView(view);
            /*{
                String mv = "select * from view_q1 ";
                String query = "select * from view_q1 where l_shipdate <= date '1998-12-01';";
                testRewriteOK(mv, query);
            }
            {
                String mv = "select *  from view_q1";
                String query = "select l_returnflag, l_shipdate, sum(sum_qty) from view_q1 group by l_returnflag, l_shipdate;";
                testRewriteOK(mv, query);
            }*/
            {
                String mv = "select l_returnflag, sum_qty  from view_q1";
                String query = "select l_returnflag, l_shipdate, sum(sum_qty) from view_q1 group by l_returnflag, l_shipdate;";
                testRewriteOK(mv, query);
            }
        }
        {

        }
    }
}
