using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections;
using System.Data;
using System.Data.SqlClient;

namespace ERPReportUtils
{
    public class FactRemainsImporter
    {
        DataClasses1DataContext dataContext;
        s1_DataClasses1DataContext dataContextS1;

        Dictionary<DateTime, int> fndDateKey = new Dictionary<DateTime, int>();
        Dictionary<Int64, Int64?> edIzmerId = new Dictionary<Int64, Int64?>();

        Int64 unitIDGramm = 18; // ID for unit Gramm

        public FactRemainsImporter()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();
        }

        // current Remains import by warehouse new and update
        public void impFactMovementsNewUpd()
        {

            DateTime now = DateTime.Now;

            HashSet<Tuple<DateTime,
                System.Data.Linq.Binary,
                System.Data.Linq.Binary,
                decimal,
                decimal, // OnStockQty
                bool,
                System.Data.Linq.Binary, // WarehouseRef
                Tuple<System.Data.Linq.Binary>>> fndRemHistFull = new HashSet<Tuple<DateTime,
                                                            System.Data.Linq.Binary,
                                                            System.Data.Linq.Binary,
                                                            decimal,
                                                            decimal, // OnStockQty
                                                            bool,
                                                            System.Data.Linq.Binary, // WarehouseRef
                                                            Tuple<System.Data.Linq.Binary>>>(); // GoodRef

            HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>> fndRemHist = new HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>>();

            Dictionary<System.Data.Linq.Binary, Int64> fndGoods = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndWarehouses = new Dictionary<System.Data.Linq.Binary, Int64>();

            // DateKey
            var fDateKey = from g in dataContext.DimDates
                           select g;
            foreach (DimDates gd in fDateKey)
            {
                fndDateKey.Add(gd.Date, gd.DateKey);
            }
            // DateKey

            // ed izm
            var edIzms = from e in dataContext.DimGoods
                         select e;
            foreach (DimGoods ed in edIzms)
            {
                edIzmerId.Add(ed.ID, ed.BaseUnitID);

            }
            // ed izm

            // Remains History
            var fRemHist = from f in dataContext.FactMovements
                           select f;
            foreach (FactMovement rh in fRemHist)
            {
                fndRemHist.Add(Tuple.Create(rh.RecorderTRef, rh.RecorderRRef, rh.LineNum));
            }
            foreach (FactMovement rh in fRemHist)
            {
                fndRemHistFull.Add(Tuple.Create(rh.Period.Value,
                                                rh.RecorderTRef,
                                                rh.RecorderRRef,
                                                rh.LineNum,
                                                rh.OnStockQty.Value,
                                                rh.Active.Value,
                                                rh.WarehouseRef,
                                                rh.GoodRef));
            }
            // Remains History

            // Goods
            var fGoods = from g in dataContext.DimGoods
                         select g;
            foreach (DimGoods gd in fGoods)
            {
                fndGoods.Add(gd.IDRRef, gd.ID);
            }
            // Goods

            // Warehouses
            var fWarehouses = from g in dataContext.DimWarehouses
                              select g;
            foreach (DimWarehouses wh in fWarehouses)
            {
                fndWarehouses.Add(wh.IDRRef, wh.ID);
            }
            // Warehouses

            // Marked active
            byte[] mActive = new byte[1];
            mActive[0] = 1;
            // Marked active

            var tovaryNaSkladah = from t in dataContextS1._AccumRg44378
                                  select t;

            int i = 0;
            foreach (_AccumRg44378 tNs in tovaryNaSkladah)
            {

                if (fndRemHistFull.Contains(Tuple.Create(tNs._Period,
                                                            tNs._RecorderTRef,
                                                            tNs._RecorderRRef,
                                                            tNs._LineNo,
                                                            tNs._Fld44385, // OnStockQty
                                                            true,
                                                            tNs._Fld44382RRef, // WarehouseRef
                                                            tNs._Fld44379RRef)) == true) // GoodRef

// !!!! TODO: сделать 2 индекса Tuple по остальным полям проверка на изменения

                { // not found or not active
                    i++;
                    continue;
                }


                if (fndRemHist.Contains(Tuple.Create(tNs._RecorderTRef, tNs._RecorderRRef, tNs._LineNo)) == true)
                {
                    // UPD
                    var hs = (from r in dataContext.FactMovements
                              where r.RecorderTRef == tNs._RecorderTRef
                              && r.RecorderRRef == tNs._RecorderRRef
                              && r.LineNum == tNs._LineNo
                              select r).FirstOrDefault();

                    // Warehouses
                    if (fndWarehouses.ContainsKey(tNs._Fld44382RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(tNs._Fld44382RRef, out val);
                        hs.WarehouseID = val;
                    }
                    else
                        throw new Exception();

                    hs.DepartmentID = null;

                    // Goods
                    if (fndGoods.ContainsKey(tNs._Fld44379RRef))
                    {
                        Int64 val;
                        fndGoods.TryGetValue(tNs._Fld44379RRef, out val);
                        hs.GoodID = val;
                    }
                    else
                        throw new Exception();

                    hs.ConsignNumber = tNs._Fld49993;
                    hs.Date = tNs._Period.AddYears(-2000);
                    hs.Period = tNs._Period;

                    // DateKey
                    if (fndDateKey.ContainsKey(hs.Date.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(hs.Date.Value.Date, out val);
                        hs.DateKey = val;
                    }
                    else
                        throw new Exception();

                    hs.RecorderTRef = tNs._RecorderTRef;
                    hs.RecorderRRef = tNs._RecorderRRef;
                    hs.LineNum = tNs._LineNo;
                    hs.RecordKind = tNs._RecordKind;
                    hs.GoodRef = tNs._Fld44379RRef;
                    hs.WarehouseRef = tNs._Fld44382RRef;
                    hs.OnStockQty = tNs._Fld44385;
                    hs.InOrdersQty = tNs._Fld44386;

                    // Units
                    var edIzmBId = (from r in edIzmerId
                                    where r.Key == hs.GoodID.Value
                                    select r).FirstOrDefault();

                    hs.BaseUnitID = edIzmBId.Value;

                    if (hs.BaseUnitID != unitIDGramm)
                    {
                        var unitConvs = (from u in dataContext.DimUnitConversion
                                         where u.FromUnitID == hs.BaseUnitID
                                         && u.ToUnitID == unitIDGramm
                                         select u).FirstOrDefault();
                        if (unitConvs == null)
                            hs.Qty = Math.Round(hs.OnStockQty.Value, 3);
                        else
                            hs.Qty = Math.Round((unitConvs.ToQty.Value * hs.OnStockQty.Value), 3);
                    }
                    else
                    {
                        hs.Qty = Math.Round(hs.OnStockQty.Value, 3);
                    }
                    hs.UnitID = unitIDGramm;

                    hs.QtyPcs = tNs._Fld49979;

                    if (tNs._Active == mActive)
                        hs.Active = true;
                    else
                        hs.Active = false;

                }
                else
                {
                    // NEW

                    FactMovement hs = new FactMovement();

                    // Warehouses
                    if (fndWarehouses.ContainsKey(tNs._Fld44382RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(tNs._Fld44382RRef, out val);
                        hs.WarehouseID = val;
                    }
                    else
                        throw new Exception();

                    hs.DepartmentID = null;

                    // Goods
                    if (fndGoods.ContainsKey(tNs._Fld44379RRef))
                    {
                        Int64 val;
                        fndGoods.TryGetValue(tNs._Fld44379RRef, out val);
                        hs.GoodID = val;
                    }
                    else
                        throw new Exception();

                    hs.ConsignNumber = tNs._Fld49993;
                    hs.Date = tNs._Period.AddYears(-2000);
                    hs.Period = tNs._Period;

                    // DateKey
                    if (fndDateKey.ContainsKey(hs.Date.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(hs.Date.Value.Date, out val);
                        hs.DateKey = val;
                    }
                    else
                        throw new Exception();

                    hs.RecorderTRef = tNs._RecorderTRef;
                    hs.RecorderRRef = tNs._RecorderRRef;
                    hs.LineNum = tNs._LineNo;
                    hs.RecordKind = tNs._RecordKind;
                    hs.GoodRef = tNs._Fld44379RRef;
                    hs.WarehouseRef = tNs._Fld44382RRef;
                    hs.OnStockQty = tNs._Fld44385;
                    hs.InOrdersQty = tNs._Fld44386;

                    // Units
                    var edIzmBId = (from r in edIzmerId
                                    where r.Key == hs.GoodID.Value
                                    select r).FirstOrDefault();

                    hs.BaseUnitID = edIzmBId.Value;

                    if (hs.BaseUnitID != unitIDGramm)
                    {
                        var unitConvs = (from u in dataContext.DimUnitConversion
                                         where u.FromUnitID == hs.BaseUnitID
                                         && u.ToUnitID == unitIDGramm
                                         select u).FirstOrDefault();
                        if (unitConvs == null)
                            hs.Qty = Math.Round(hs.OnStockQty.Value, 3);
                        else
                            hs.Qty = Math.Round((unitConvs.ToQty.Value * hs.OnStockQty.Value), 3);
                    }
                    else
                    {
                        hs.Qty = Math.Round(hs.OnStockQty.Value, 3);
                    }
                    hs.UnitID = unitIDGramm;

                    hs.QtyPcs = tNs._Fld49979;

                    if (tNs._Active == mActive)
                        hs.Active = true;
                    else
                        hs.Active = false;

                    dataContext.FactMovements.InsertOnSubmit(hs);

                } // else

                if (i % 250 == 0)
                    dataContext.SubmitChanges();
                i++;

            } // foreach (_AccumRg44378 tNs in tovaryNaSkladah)
            dataContext.SubmitChanges();

        } // public void impFactMovementsNew()

        // current Remains import by warehouse deleted
        public void impFactMovementsDel()
        {

            DateTime now = DateTime.Now;

            HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>> loadRemHist = new HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>>();

            // From 1C tovary na skladah
            var tovaryNaSkladah = from t in dataContextS1._AccumRg44378
                                  select t;
            foreach (_AccumRg44378 rh in tovaryNaSkladah)
            {
                loadRemHist.Add(Tuple.Create(rh._RecorderTRef, rh._RecorderRRef, rh._LineNo));
            }
            // From 1C tovary na skladah

            var fRemHist = from f in dataContext.FactMovements
                           select f;

            int i = 0;
            foreach (FactMovement rh in fRemHist)
            {

                if (loadRemHist.Contains(Tuple.Create(rh.RecorderTRef, rh.RecorderRRef, rh.LineNum)) == true)
                {
                    i++;
                    continue;
                }

                //rh.Active = false;
                dataContext.FactMovements.DeleteOnSubmit(rh);

                if (i % 50 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        } // public void impFactMovementsDel()

        public void fromHistoryToRemains()
        {
            DateTime toDateCount = new DateTime(2016, 06, 01);


            //--------> DELETE ALL REMAINS 
            deleteAllRemains();

            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;
            DateTime onDate = DateTime.Today; // Remains on current date

            Dictionary<Int64, string> edIzmer = new Dictionary<Int64, string>();
            Dictionary<Int64, Int64?> edIzmerId = new Dictionary<Int64, Int64?>();
            long edIzmerGram = new long();

            Dictionary<Int64, decimal> nominalWghts = new Dictionary<Int64, decimal>();

            // ed izm
            var edIzms = from e in dataContext.DimGoods
                         select e;
            foreach (DimGoods ed in edIzms)
            {
                edIzmerId.Add(ed.ID, ed.BaseUnitID);
                edIzmer.Add(ed.ID, ed.BaseUnit);
            }
            edIzmerGram = (from e in dataContext.DimUnits
                           where e.TypeName == "г"
                           && e.Active == true
                           select e.ID).First();
            // ed izm

            // NominalWeights
            var nomWts = from n in dataContext.DimGoods
                         where n.WeightGrammNominal != null
                         select n;
            foreach (DimGoods nw in nomWts)
            {
                nominalWghts.Add(nw.ID, nw.WeightGrammNominal.Value);
            }
            // NominalWeights

            // Delete on date
            deleteRemainsOnDate(onDate); // Fast delete from DB
            //var onDateRemains = from r in dataContext.FactRemains
            //                    where r.RemainsDate == onDate
            //                    select r;
            //dataContext.FactRemains.DeleteAllOnSubmit(onDateRemains);
            //dataContext.SubmitChanges();
            // Delete on date

            var dimDates = (from d in dataContext.DimDates
                            where d.Date == onDate
                            select d).FirstOrDefault();
            if (dimDates == null)
                throw new Exception();


            var remHistIn = from f in dataContext.FactMovements
                            where f.RecordKind == 0
                            && f.Active == true
                            //&& f.Date <= toDateCount
                            group f by new
                            {
                                f.WarehouseID,
                                f.GoodID,
                                f.ConsignNumber,
                                f.Date
                            }
                          into g
                            select new
                            {
                                WarehouseID = g.Key.WarehouseID,
                                GoodID = g.Key.GoodID,
                                Date = g.Key.Date,
                                ConsignNumber = g.Key.ConsignNumber,
                                OnStockQty = g.Sum(t => t.OnStockQty),
                                InOrdersQty = g.Sum(t => t.InOrdersQty)
                            };
            var remHistOut = from f in dataContext.FactMovements
                             where f.RecordKind == 1
                             && f.Active == true
                             //&& f.Date <= toDateCount
                             group f by new
                             {
                                 f.WarehouseID,
                                 f.GoodID,
                                 f.ConsignNumber,
                                 f.Date
                             }
                            into g
                             select new
                             {
                                 WarehouseID = g.Key.WarehouseID,
                                 GoodID = g.Key.GoodID,
                                 Date = g.Key.Date,
                                 ConsignNumber = g.Key.ConsignNumber,
                                 OnStockQty = g.Sum(t => t.OnStockQty) * (-1),
                                 InOrdersQty = g.Sum(t => t.InOrdersQty) * (-1)
                             };
            var remHistT = (from r in remHistIn
                            where r.OnStockQty != 0
                            select r).Concat(from r in remHistOut
                                             where r.OnStockQty != 0
                                             select r);
            var remHist = from r in remHistT
                          where r.OnStockQty != 0
                          group r by new
                          {
                              r.WarehouseID,
                              r.GoodID,
                              r.ConsignNumber
                          }
                            into g
                          select new
                          {
                              WarehouseID = g.Key.WarehouseID,
                              GoodID = g.Key.GoodID,
                              ConsignNumber = g.Key.ConsignNumber,
                              OnStockQty = g.Sum(t => t.OnStockQty),
                              InOrdersQty = g.Sum(t => t.InOrdersQty)
                          };

            int i = 0;
            foreach (var rh in remHist)
            {
                FactRemains rm = new FactRemains();
                rm.WarehouseID = rh.WarehouseID;
                rm.DepartmentID = null;
                rm.GoodID = rh.GoodID;
                rm.ConsignNumber = rh.ConsignNumber;
                rm.RemainsDate = onDate;
                rm.DateKey = dimDates.DateKey;
                rm.OnStockQty = rh.OnStockQty;
                rm.InOrdersQty = rh.InOrdersQty;

                // Unit
                var edIzmB = (from r in edIzmer
                              where r.Key == rh.GoodID
                              select r).FirstOrDefault();
                var edIzmBId = (from r in edIzmerId
                              where r.Key == rh.GoodID
                              select r).FirstOrDefault();

                if (edIzmB.Value != null)
                {
                    rm.BaseUnitID = edIzmBId.Value; // don't move down!

                    switch (edIzmB.Value)
                    {
                        case "шт":
                            rm.QtyGram = 0;
                            rm.QtyPcs = rh.OnStockQty;
                            break;

                        case "г":
                            rm.QtyPcs = 0;
                            rm.QtyGram = rh.OnStockQty;
                            {
                                decimal n;
                                if (nominalWghts.TryGetValue(rm.GoodID.Value, out n) == true)
                                {
                                    if (n != 0)
                                    {
                                        rm.QtyPcs = rm.QtyGram / n;
                                    }
                                }
                            }
                            break;

                        case "кг":
                            rm.QtyPcs = 0;
                            rm.QtyGram = rh.OnStockQty * (1000);
                            rm.BaseUnitID = edIzmerGram; // repair from kg to g
                            {
                                decimal n;
                                if (nominalWghts.TryGetValue(rm.GoodID.Value, out n) == true)
                                {
                                    if (n != 0)
                                    {
                                        rm.QtyPcs = rm.QtyGram / n;
                                    }
                                }
                            }
                            break;

                        default:
                            rm.QtyPcs = rh.OnStockQty;
                            rm.QtyGram = 0;
                            break;
                    }
                }
                // Unit

                rm.Active = true;

                dataContext.FactRemains.InsertOnSubmit(rm);

                if (i % 250 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        } // public void fromHistoryToRemains()

        private void deleteRemainsOnDate(DateTime onDate)
        {
            SqlConnection sqlConnection1 = new SqlConnection(global::ERPReportUtils.Properties.Settings.Default.ERPReportDBConnectionString);
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;

            cmd.CommandText = "DELETE FROM [APReport].[dbo].[FactRemains] WHERE RemainsDate = '" + onDate.ToString() + "'";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();

            reader = cmd.ExecuteReader();
            // Data is accessible through the DataReader object here.

            sqlConnection1.Close();
        }

        private void deleteAllRemains()
        {
            SqlConnection sqlConnection1 = new SqlConnection(global::ERPReportUtils.Properties.Settings.Default.ERPReportDBConnectionString);
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;

            cmd.CommandText = "DELETE FROM [APReport].[dbo].[FactRemains]";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();

            reader = cmd.ExecuteReader();
            // Data is accessible through the DataReader object here.

            sqlConnection1.Close();
        }

    } // public class FactImporter

}
