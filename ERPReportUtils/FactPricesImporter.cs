using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace ERPReportUtils
{
    public class FactPricesImporter
    {
        DataClasses1DataContext dataContext;
        s1_DataClasses1DataContext dataContextS1;

        public FactPricesImporter()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();
        }

        // Prices import
        public void impFactPrices()
        {

            DateTime now = DateTime.Now;

            HashSet<Tuple<DateTime,
                            System.Data.Linq.Binary, // TRef
                            System.Data.Linq.Binary, // RRef
                            decimal, // LineNo
                            decimal, // Price
                            bool, // Active
                            System.Data.Linq.Binary // GoodRef
                            >> fndPricesFull = new HashSet<Tuple<DateTime,
                                                System.Data.Linq.Binary,
                                                System.Data.Linq.Binary,
                                                decimal,
                                                decimal,
                                                bool,
                                                System.Data.Linq.Binary>>();

            HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>> fndPrices = new HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>>();

            Dictionary<System.Data.Linq.Binary, Int64> fndGoods = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndUnits = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndCurrencies = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndPriceTypes = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<DateTime, int> fndDateKey = new Dictionary<DateTime, int>();

            // Price History
            var fPricesHist = from f in dataContext.FactPricesHistory
                              select f;
            foreach (FactPricesHistory rh in fPricesHist)
            {
                fndPrices.Add(Tuple.Create(rh.RecorderTRef, rh.RecorderRRef, rh.LineNum));
            }
            foreach (FactPricesHistory rh in fPricesHist)
            {
                fndPricesFull.Add(Tuple.Create(rh.Period.Value,
                                                rh.RecorderTRef,
                                                rh.RecorderRRef,
                                                rh.LineNum,
                                                rh.Price,
                                                rh.Active.Value,
                                                rh.GoodRef));
            }
            // Price History

            // Goods
            var fGoods = from g in dataContext.DimGoods
                         select g;
            foreach (DimGoods gd in fGoods)
            {
                fndGoods.Add(gd.IDRRef, gd.ID);
            }
            // Goods

            // Units
            var fUnits = from g in dataContext.DimUnits
                         select g;
            foreach (DimUnits gd in fUnits)
            {
                fndUnits.Add(gd.IDRRef, gd.ID);
            }
            // Units

            // Currencies
            var fCurrencies = from g in dataContext.DimCurrencies
                              select g;
            foreach (DimCurrencies gd in fCurrencies)
            {
                fndCurrencies.Add(gd.IDRRef, gd.ID);
            }
            // Currencies

            // PriceTypes
            var fPriceTypes = from g in dataContext.DimPriceTypes
                              select g;
            foreach (DimPriceTypes gd in fPriceTypes)
            {
                fndPriceTypes.Add(gd.IDRRef, gd.ID);
            }
            // PriceTypes

            // DateKey
            var fDateKey = from g in dataContext.DimDates
                           select g;
            foreach (DimDates gd in fDateKey)
            {
                fndDateKey.Add(gd.Date, gd.DateKey);
            }
            // DateKey

            // Marked active
            byte[] mActive = new byte[1];
            mActive[0] = 1;
            // Marked active

            var ceny = from c in dataContextS1._InfoRg41332
                       select c;

            int i = 0;
            foreach (_InfoRg41332 tNs in ceny)
            {

                if (fndPricesFull.Contains(Tuple.Create(tNs._Period,
                                            tNs._RecorderTRef,
                                            tNs._RecorderRRef,
                                            tNs._LineNo,
                                            tNs._Fld41336, // Price
                                            true,
                                            tNs._Fld41333RRef)) == true)
                { // not found or not active
                    i++;
                    continue;
                }

                if (fndPrices.Contains(Tuple.Create(tNs._RecorderTRef, tNs._RecorderRRef, tNs._LineNo)) == true)
                {
                    // UPD

                    var fp = (from r in dataContext.FactPricesHistory
                              where r.RecorderTRef == tNs._RecorderTRef
                              && r.RecorderRRef == tNs._RecorderRRef
                              && r.LineNum == tNs._LineNo
                              select r).FirstOrDefault();


                    // Goods
                    if (fndGoods.ContainsKey(tNs._Fld41333RRef))
                    {
                        Int64 val;
                        fndGoods.TryGetValue(tNs._Fld41333RRef, out val);
                        fp.GoodID = val;
                    }
                    else
                        throw new Exception();

                    // Units
                    if (fndUnits.ContainsKey(tNs._Fld41337RRef))
                    {
                        Int64 val;
                        fndUnits.TryGetValue(tNs._Fld41337RRef, out val);
                        fp.UnitID = val;
                    }
                    //else
                    //    throw new Exception();

                    // Currencies
                    if (fndCurrencies.ContainsKey(tNs._Fld41338RRef))
                    {
                        Int64 val;
                        fndCurrencies.TryGetValue(tNs._Fld41338RRef, out val);
                        fp.CurrencyID = val;
                    }
                    else
                        throw new Exception();

                    // PriceTypes
                    if (fndPriceTypes.ContainsKey(tNs._Fld41334RRef))
                    {
                        Int64 val;
                        fndPriceTypes.TryGetValue(tNs._Fld41334RRef, out val);
                        fp.PriceTypeID = val;
                    }
                    else
                        throw new Exception();

                    fp.Date = tNs._Period.AddYears(-2000); // !!! before DateKey

                    // DateKey
                    if (fndDateKey.ContainsKey(fp.Date.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(fp.Date.Value.Date, out val);
                        fp.DateKey = val;
                    }
                    else
                        throw new Exception();

                    fp.Price = tNs._Fld41336;
                    fp.Period = tNs._Period;
                    fp.RecorderTRef = tNs._RecorderTRef;
                    fp.RecorderRRef = tNs._RecorderRRef;
                    fp.LineNum = tNs._LineNo;
                    fp.GoodRef = tNs._Fld41333RRef;
                    if (tNs._Active == mActive)
                        fp.Active = true;
                    else
                        fp.Active = false;

                }
                else
                {
                    // NEW

                    FactPricesHistory fp = new FactPricesHistory();

                    // Goods
                    if (fndGoods.ContainsKey(tNs._Fld41333RRef))
                    {
                        Int64 val;
                        fndGoods.TryGetValue(tNs._Fld41333RRef, out val);
                        fp.GoodID = val;
                    }
                    else
                        throw new Exception();

                    // Units
                    if (fndUnits.ContainsKey(tNs._Fld41337RRef))
                    {
                        Int64 val;
                        fndUnits.TryGetValue(tNs._Fld41337RRef, out val);
                        fp.UnitID = val;
                    }
                    //else
                    //    throw new Exception();

                    // Currencies
                    if (fndCurrencies.ContainsKey(tNs._Fld41338RRef))
                    {
                        Int64 val;
                        fndCurrencies.TryGetValue(tNs._Fld41338RRef, out val);
                        fp.CurrencyID = val;
                    }
                    else
                        throw new Exception();

                    // PriceTypes
                    if (fndPriceTypes.ContainsKey(tNs._Fld41334RRef))
                    {
                        Int64 val;
                        fndPriceTypes.TryGetValue(tNs._Fld41334RRef, out val);
                        fp.PriceTypeID = val;
                    }
                    else
                        throw new Exception();

                    fp.Date = tNs._Period.AddYears(-2000); // !!! before DateKey

                    // DateKey
                    if (fndDateKey.ContainsKey(fp.Date.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(fp.Date.Value.Date, out val);
                        fp.DateKey = val;
                    }
                    else
                        throw new Exception();

                    fp.Price = tNs._Fld41336;
                    fp.Period = tNs._Period;
                    fp.RecorderTRef = tNs._RecorderTRef;
                    fp.RecorderRRef = tNs._RecorderRRef;
                    fp.LineNum = tNs._LineNo;
                    fp.GoodRef = tNs._Fld41333RRef;
                    if (tNs._Active == mActive)
                        fp.Active = true;
                    else
                        fp.Active = false;

                    dataContext.FactPricesHistory.InsertOnSubmit(fp);
                }

                if (i % 250 == 0)
                    dataContext.SubmitChanges();
                i++;
            } // foreach (_InfoRg41332 tNs in ceny)
            dataContext.SubmitChanges();

        } // public void impFactPrices()

        public void impFactPricesDel()
        {
            DateTime now = DateTime.Now;

            HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>> loadPricesHist = new HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>>();

            // From 1C 
            var ceny = from t in dataContextS1._InfoRg41332
                       select t;
            foreach (_InfoRg41332 price in ceny)
            {
                loadPricesHist.Add(Tuple.Create(price._RecorderTRef, price._RecorderRRef, price._LineNo));
            }
            // From 1C 

            var fPriceHist = from f in dataContext.FactPricesHistory
                             select f;

            int i = 0;
            foreach (FactPricesHistory rh in fPriceHist)
            {

                if (loadPricesHist.Contains(Tuple.Create(rh.RecorderTRef, rh.RecorderRRef, rh.LineNum)) == true)
                {
                    i++;
                    continue;
                }

                //rh.Active = false;
                dataContext.FactPricesHistory.DeleteOnSubmit(rh);

                if (i % 50 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        } // public void impFactPricesDel()

        public void fromHistoryToPrices()
        {
            //--------> DELETE ALL PRICES 
            deleteAllPrices();


            DateTime now = DateTime.Now;
            DateTime onDate = DateTime.Today; // Prices on current date

            HashSet<Int64> fndGoods = new HashSet<Int64>();
            Dictionary<Int64, decimal> fndWeight = new Dictionary<Int64, decimal>();

            // Good weights
            var fWeight = from g in dataContext.DimGoods
                          select g;
            foreach (DimGoods gd in fWeight)
            {
                if (gd.WeightGramm1Pcs != null)
                    fndWeight.Add(gd.ID, gd.WeightGramm1Pcs.Value);
            }
            // Good weights

            // Delete on date
            deletePricesOnDate(onDate); // Fast delete from DB

            var dimDatesOnDate = (from d in dataContext.DimDates
                                  where d.Date == onDate
                                  select d).FirstOrDefault();
            if (dimDatesOnDate == null)
                throw new Exception();

            var priceHist = (from f in dataContext.FactPricesHistory
                             where f.PriceTypeID == 3 // only retail (roznichnaya) price
                             orderby f.GoodID ascending, f.Date descending // only last by date
                             select f);


            int i = 0;
            foreach (var ph in priceHist)
            {

                if (fndGoods.Contains(ph.GoodID.Value) == true)
                {
                    i++;
                    continue;
                }

                FactPrices pn = new FactPrices();
                pn.GoodID = ph.GoodID;
                pn.OnDate = onDate;
                pn.DateKey = dimDatesOnDate.DateKey;
                pn.PriceDate = ph.Date;
                pn.PriceDateKey = ph.DateKey;
                pn.Price = ph.Price;

                // Goods
                if (fndWeight.ContainsKey(ph.GoodID.Value))
                {
                    decimal val;
                    fndWeight.TryGetValue(ph.GoodID.Value, out val);
                    if (val > 0)
                        pn.PricePerUnit = ph.Price / val;
                    else
                        pn.PricePerUnit = ph.Price;
                }
                else
                    pn.PricePerUnit = ph.Price;

                pn.UnitID = ph.UnitID;
                pn.BaseUnitID = ph.DimGoods.BaseUnitID;

                pn.CurrencyID = ph.CurrencyID;
                pn.PriceTypeID = ph.PriceTypeID;

                pn.Active = true;

                dataContext.FactPrices.InsertOnSubmit(pn);

                fndGoods.Add(ph.GoodID.Value);

                if (i % 250 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        } // public void fromHistoryToPrices()

        private void deletePricesOnDate(DateTime onDate)
        {
            SqlConnection sqlConnection1 = new SqlConnection(global::ERPReportUtils.Properties.Settings.Default.ERPReportDBConnectionString);
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;

            cmd.CommandText = "DELETE FROM [APReport].[dbo].[FactPrices] WHERE OnDate = '" + onDate.ToString() + "'";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();

            reader = cmd.ExecuteReader();
            // Data is accessible through the DataReader object here.

            sqlConnection1.Close();
        }

        private void deleteAllPrices()
        {
            SqlConnection sqlConnection1 = new SqlConnection(global::ERPReportUtils.Properties.Settings.Default.ERPReportDBConnectionString);
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;

            cmd.CommandText = "DELETE FROM [APReport].[dbo].[FactPrices]";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();

            reader = cmd.ExecuteReader();
            // Data is accessible through the DataReader object here.

            sqlConnection1.Close();
        }

    } // public class FactPricesImporter
}
