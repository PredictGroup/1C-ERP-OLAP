using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ERPReportUtils
{
    public class DocSalesImporter
    {
        DataClasses1DataContext dataContext;
        s1_DataClasses1DataContext dataContextS1;

        Dictionary<System.Data.Linq.Binary, Int64> fndGoods = new Dictionary<System.Data.Linq.Binary, Int64>();
        Dictionary<Int64, Int64?> edIzmerId = new Dictionary<Int64, Int64?>();

        Dictionary<System.Data.Linq.Binary, Int64> fndWarehouses = new Dictionary<System.Data.Linq.Binary, Int64>();
        Dictionary<System.Data.Linq.Binary, Int64> fndCurrencies = new Dictionary<System.Data.Linq.Binary, Int64>();
        Dictionary<System.Data.Linq.Binary, Int64> fndPartner = new Dictionary<System.Data.Linq.Binary, Int64>();

        Dictionary<DateTime, int> fndDateKey = new Dictionary<DateTime, int>();

        Int64 unitIDGramm = 18; // ID for unit Gramm
        Int64 currIDRubl = 4; // ID for rubles

        FactCurrencyRate[] CurrRates;


        public DocSalesImporter()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();
        }

        public void impDocSales()
        {
            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndDocs = new HashSet<System.Data.Linq.Binary>();
            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                            System.Data.Linq.Binary>> fndDocVer = new HashSet<Tuple<System.Data.Linq.Binary,
                            System.Data.Linq.Binary>>();


            // Fill documents
            var docs = from d in dataContext.DocSales
                       select d;
            foreach (DocSale doc in docs)
            {
                fndDocs.Add(doc.IDRRef);
            }
            foreach (DocSale doc in docs)
            {
                fndDocVer.Add(Tuple.Create(doc.IDRRef, doc.Version));
            }
            // Fill documents


            // Warehouses
            var fWarehouses = from g in dataContext.DimWarehouses
                              select g;
            foreach (DimWarehouses wh in fWarehouses)
            {
                fndWarehouses.Add(wh.IDRRef, wh.ID);
            }
            // Warehouses

            // Currencies
            var fCurrencies = from g in dataContext.DimCurrencies
                              select g;
            foreach (DimCurrencies gd in fCurrencies)
            {
                fndCurrencies.Add(gd.IDRRef, gd.ID);
            }
            CurrRates = (from c in dataContext.FactCurrencyRates
                         select c).ToArray();
            // Currencies

            // Partners
            var fPartner = from p in dataContext.DimPartners
                           select p;
            foreach (DimPartner dp in fPartner)
            {
                fndPartner.Add(dp.IDRRef, dp.ID);
            }
            // Partners

            // DateKey
            var fDateKey = from g in dataContext.DimDates
                           select g;
            foreach (DimDates gd in fDateKey)
            {
                fndDateKey.Add(gd.Date, gd.DateKey);
            }
            // DateKey


            // for lines
            // Goods
            var fGoods = from g in dataContext.DimGoods
                         select g;
            foreach (DimGoods gd in fGoods)
            {
                fndGoods.Add(gd.IDRRef, gd.ID);
            }
            // Goods

            // ed izm
            var edIzms = from e in dataContext.DimGoods
                         select e;
            foreach (DimGoods ed in edIzms)
            {
                edIzmerId.Add(ed.ID, ed.BaseUnitID);

            }
            // ed izm

            // Marked deleted
            byte[] posted = new byte[1];
            posted[0] = 1;
            // Marked deleted

            var salesDocs = from c in dataContextS1._Document822s
                            where c._Posted == posted
                            select c;
            int i = 0;
            foreach (_Document822 doc in salesDocs)
            {

                if (fndDocVer.Contains(Tuple.Create(doc._IDRRef, doc._Version)) == true)
                {
                    i++;
                    continue;
                }

                if (fndDocs.Contains(doc._IDRRef) == true)
                {
                    // UPD
                    var dp = (from n in dataContext.DocSales
                              where n.IDRRef == doc._IDRRef
                              select n).FirstOrDefault();

                    dp.NumberPrefix = doc._NumberPrefix;
                    dp.DocNumber = doc._Number;

                    // Warehouses
                    if (fndWarehouses.ContainsKey(doc._Fld29564RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(doc._Fld29564RRef, out val);
                        dp.WarehouseID = val;
                    }
                    //else
                    //    throw new Exception();

                    dp.DocDate = doc._Date_Time.AddYears(-2000);

                    // DateKey
                    if (fndDateKey.ContainsKey(dp.DocDate.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(dp.DocDate.Value.Date, out val);
                        dp.DateKey = val;
                    }
                    else
                        throw new Exception();

                    dp.Posted = doc._Posted;

                    // Partners
                    if (fndPartner.ContainsKey(doc._Fld29560RRef))
                    {
                        Int64 val;
                        fndPartner.TryGetValue(doc._Fld29560RRef, out val);
                        dp.PartnerID = val;
                    }
                    else
                        throw new Exception();

                    // Currencies
                    if (fndCurrencies.ContainsKey(doc._Fld29544RRef))
                    {
                        Int64 val;
                        fndCurrencies.TryGetValue(doc._Fld29544RRef, out val);
                        dp.CurrencyID = val;
                    }
                    else
                        throw new Exception();

                    if (dp.CurrencyID == currIDRubl)
                        dp.DocAmount = doc._Fld29559;
                    else
                    {
                        var rate = (from r in CurrRates
                                    where r.CurrencyID == dp.CurrencyID
                                    && r.Date >= dp.DocDate
                                    select r).FirstOrDefault();

                        dp.DocAmount = doc._Fld29559 * rate.Rate.Value / rate.Fold.Value;
                    }

                    dp.Version = doc._Version;

                    if (doc._Posted == posted)
                        dp.Active = true;
                    else
                        dp.Active = false;
                    dataContext.SubmitChanges();

                    // update lines
                    var lines = from l in dataContext.DocSalesLines
                                where l.Document822_IDRRef == doc._IDRRef
                                select l;
                    dataContext.DocSalesLines.DeleteAllOnSubmit(lines);
                    dataContext.SubmitChanges();
                    impDocSalesLines(doc._IDRRef, dp.CurrencyID.Value, dp.DocDate.Value); // import lines
                }
                else
                {
                    // NEW
                    DocSale dp = new DocSale();
                    dp.NumberPrefix = doc._NumberPrefix;
                    dp.DocNumber = doc._Number;

                    // Warehouses
                    if (fndWarehouses.ContainsKey(doc._Fld29564RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(doc._Fld29564RRef, out val);
                        dp.WarehouseID = val;
                    }
                    //else
                    //throw new Exception();

                    dp.DocDate = doc._Date_Time.AddYears(-2000);

                    // DateKey
                    if (fndDateKey.ContainsKey(dp.DocDate.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(dp.DocDate.Value.Date, out val);
                        dp.DateKey = val;
                    }
                    else
                        throw new Exception();

                    dp.Posted = doc._Posted;

                    // Partners
                    if (fndPartner.ContainsKey(doc._Fld29560RRef))
                    {
                        Int64 val;
                        fndPartner.TryGetValue(doc._Fld29560RRef, out val);
                        dp.PartnerID = val;
                    }
                    else
                        throw new Exception();

                    // Currencies
                    if (fndCurrencies.ContainsKey(doc._Fld29544RRef))
                    {
                        Int64 val;
                        fndCurrencies.TryGetValue(doc._Fld29544RRef, out val);
                        dp.CurrencyID = val;
                    }
                    else
                        throw new Exception();

                    if (dp.CurrencyID == currIDRubl)
                        dp.DocAmount = doc._Fld29559;
                    else
                    {
                        var rate = (from r in CurrRates
                                    where r.CurrencyID == dp.CurrencyID
                                    && r.Date <= dp.DocDate
                                    orderby r.Date descending
                                    select r).FirstOrDefault();

                        dp.DocAmount = doc._Fld29559 * rate.Rate.Value / rate.Fold.Value;
                    }


                    dp.IDRRef = doc._IDRRef;
                    dp.Version = doc._Version;

                    if (doc._Posted == posted)
                        dp.Active = true;
                    else
                        dp.Active = false;
                    dataContext.DocSales.InsertOnSubmit(dp);
                    dataContext.SubmitChanges();

                    impDocSalesLines(doc._IDRRef, dp.CurrencyID.Value, dp.DocDate.Value); // import lines

                } // if (fndDocs.Contains(doc._IDRRef) == true)

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;

            } // foreach (_Document581 doc in prodDocs)

            dataContext.SubmitChanges();
        }

        private void impDocSalesLines(System.Data.Linq.Binary docIDRRef, Int64 CurrencyID, DateTime DocDate)
        {

            var doc = (from c in dataContext.DocSales
                       where c.IDRRef == docIDRRef
                       select c).FirstOrDefault();

            var docLns = from c in dataContextS1._Document822_VT29611s
                         where c._Document822_IDRRef == docIDRRef
                         select c;

            int i = 0;
            foreach (_Document822_VT29611 ln in docLns)
            {
                DocSalesLine nl = new DocSalesLine();
                nl.DocID = doc.ID;

                // Goods
                if (fndGoods.ContainsKey(ln._Fld29613RRef))
                {
                    Int64 val;
                    fndGoods.TryGetValue(ln._Fld29613RRef, out val);
                    nl.GoodID = val;
                }
                else
                    throw new Exception();

                // Warehouses
                if (fndWarehouses.ContainsKey(ln._Fld29631RRef))
                {
                    Int64 val;
                    fndWarehouses.TryGetValue(ln._Fld29631RRef, out val);
                    nl.WarehouseID = val;
                }
                //else
                //throw new Exception();

                nl.BaseQty = ln._Fld29618;

                // Units
                var edIzmBId = (from r in edIzmerId
                                where r.Key == nl.GoodID.Value
                                select r).FirstOrDefault();

                nl.BaseUnitID = edIzmBId.Value;

                if (nl.BaseUnitID != unitIDGramm)
                {
                    var unitConvs = (from u in dataContext.DimUnitConversion
                                     where u.FromUnitID == nl.BaseUnitID
                                     && u.ToUnitID == unitIDGramm
                                     select u).FirstOrDefault();
                    if (unitConvs == null)
                        nl.Qty = Math.Round(nl.BaseQty.Value, 3);
                    else
                        nl.Qty = Math.Round((unitConvs.ToQty.Value * nl.BaseQty.Value), 3);
                }
                else
                {
                    nl.Qty = Math.Round(nl.BaseQty.Value, 3);
                }

                nl.UnitID = unitIDGramm;

                nl.QtyPcs = ln._Fld49969;

                if (CurrencyID == currIDRubl)
                {
                    nl.Price = ln._Fld29620;
                    nl.Amount = ln._Fld29621;

                    nl.AmountTax = ln._Fld29623;
                    nl.AmountWithTax = ln._Fld29624;

                    nl.DiscountAutoAmount = ln._Fld29627;
                    nl.DiscountManualAmount = ln._Fld29626;
                }
                else
                {
                    var rate = (from r in CurrRates
                                where r.CurrencyID == CurrencyID
                                && r.Date <= DocDate
                                orderby r.Date descending
                                select r).FirstOrDefault();

                    nl.Price = ln._Fld29620 * rate.Rate.Value / rate.Fold.Value;
                    nl.Amount = ln._Fld29621 * rate.Rate.Value / rate.Fold.Value;

                    nl.AmountTax = ln._Fld29623 * rate.Rate.Value / rate.Fold.Value;
                    nl.AmountWithTax = ln._Fld29624 * rate.Rate.Value / rate.Fold.Value;

                    nl.DiscountAutoAmount = ln._Fld29627 * rate.Rate.Value / rate.Fold.Value;
                    nl.DiscountManualAmount = ln._Fld29626 * rate.Rate.Value / rate.Fold.Value;
                }

                nl.DiscountAutoPercent = ln._Fld29629;
                nl.DiscountManualPercent = ln._Fld29628;

                nl.Document822_IDRRef = ln._Document822_IDRRef;
                nl.KeyField = ln._KeyField;
                nl.LineNum = ln._LineNo29612;
                nl.Active = true;
                dataContext.DocSalesLines.InsertOnSubmit(nl);

            } // foreach (_Document581_VT15877 ln in docLns)
        }

        public void impDocSalesDel()
        {
            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> loadDocHist = new HashSet<System.Data.Linq.Binary>();

            // Marked deleted
            byte[] posted = new byte[1];
            posted[0] = 1;
            // Marked deleted

            // From 1C 
            var posDocs = from t in dataContextS1._Document822s
                          where t._Posted == posted
                          select t;

            foreach (_Document822 dc in posDocs)
            {
                loadDocHist.Add(dc._IDRRef);
            }
            // From 1C tovary na skladah

            var fDocHists = from f in dataContext.DocSales
                            select f;

            int i = 0;
            foreach (DocSale dc in fDocHists)
            {

                if (loadDocHist.Contains(dc.IDRRef) == true)
                {
                    i++;
                    continue;
                }

                //dc.Active = false;
                impDocSalesLinesDel(dc.IDRRef);
                dataContext.DocSales.DeleteOnSubmit(dc);

                if (i % 50 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        } // public void impDocSalesDel()

        private void impDocSalesLinesDel(System.Data.Linq.Binary docIDRRef)
        {
            var dLns = from c in dataContext.DocSalesLines
                       where c.Document822_IDRRef == docIDRRef
                       select c;

            dataContext.DocSalesLines.DeleteAllOnSubmit(dLns);
        }

    } // public class DocSalesImporter
}
