using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ERPReportUtils
{
    public class FactPOSClass
    {
        DataClasses1DataContext dataContext;
        s1_DataClasses1DataContext dataContextS1;

        Dictionary<System.Data.Linq.Binary, Int64> fndGoods = new Dictionary<System.Data.Linq.Binary, Int64>();
        Dictionary<Int64, Int64?> edIzmerId = new Dictionary<Int64, Int64?>();
        Dictionary<System.Data.Linq.Binary, Int64> fndPeople = new Dictionary<System.Data.Linq.Binary, Int64>();

        public FactPOSClass()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();
        }

        public void impFactPOS()
        {

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndDocs = new HashSet<System.Data.Linq.Binary>();
            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                            System.Data.Linq.Binary>> fndDocVer = new HashSet<Tuple<System.Data.Linq.Binary,
                            System.Data.Linq.Binary>>();

            Dictionary<System.Data.Linq.Binary, Int64> fndWarehouses = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndCurrencies = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndPriceTypes = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndRegisters = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, string> fndCards = new Dictionary<System.Data.Linq.Binary, string>();

            Dictionary<DateTime, int> fndDateKey = new Dictionary<DateTime, int>();

            // Fill documents
            var docs = from d in dataContext.FactPOS
                       select d;
            foreach (FactPOS doc in docs)
            {
                fndDocs.Add(doc.IDRRef);
            }
            foreach (FactPOS doc in docs)
            {
                fndDocVer.Add(Tuple.Create(doc.IDRRef, doc.Version));
            }
            // Fill documents

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

            // Warehouses
            var fWarehouses = from g in dataContext.DimWarehouses
                              select g;
            foreach (DimWarehouses wh in fWarehouses)
            {
                fndWarehouses.Add(wh.IDRRef, wh.ID);
            }
            // Warehouses

            // People
            var fPeople = from p in dataContext.DimPeople
                          select p;
            foreach (DimPeople dp in fPeople)
            {
                fndPeople.Add(dp.IDRRef, dp.ID);
            }
            // People

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

            // POS Registers
            var fRegisters = from g in dataContext.DimPOSRegisters
                             select g;
            foreach (DimPOSRegisters gd in fRegisters)
            {
                fndRegisters.Add(gd.IDRRef, gd.ID);
            }
            // POS Registers

            // Loyal cards
            var fCards = from c in dataContextS1._Reference177
                         select c;
            foreach (_Reference177 crd in fCards)
            {
                fndCards.Add(crd._IDRRef, crd._Description);
            }
            // Loyal cards

            // DateKey
            var fDateKey = from g in dataContext.DimDates
                           select g;
            foreach (DimDates gd in fDateKey)
            {
                fndDateKey.Add(gd.Date, gd.DateKey);
            }
            // DateKey

            // Marked deleted
            byte[] posted = new byte[1];
            posted[0] = 1;
            // Marked deleted

            var cheques = from c in dataContextS1._Document898
                          where c._Posted == posted
                          select c;

            int i = 0;
            foreach (_Document898 cheque in cheques)
            {

                if (fndDocVer.Contains(Tuple.Create(cheque._IDRRef, cheque._Version)) == true)
                {
                    i++;
                    continue;
                }

                if (fndDocs.Contains(cheque._IDRRef) == true)
                {
                    // UPD

                    var nf = (from n in dataContext.FactPOS
                              where n.IDRRef == cheque._IDRRef
                              select n).FirstOrDefault();
                    nf.NumberPrefix = cheque._NumberPrefix;
                    nf.ChequeNumber = cheque._Number;
                    nf.DocDate = cheque._Date_Time.AddYears(-2000);

                    // DateKey
                    if (fndDateKey.ContainsKey(nf.DocDate.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(nf.DocDate.Value.Date, out val);
                        nf.DateKey = val;
                    }
                    else
                        throw new Exception();

                    nf.Posted = cheque._Posted;

                    // People
                    if (fndPeople.ContainsKey(cheque._Fld33139RRef))
                    {
                        Int64 val;
                        fndPeople.TryGetValue(cheque._Fld33139RRef, out val);
                        nf.CasherID = val;
                    }
                    else
                        throw new Exception();

                    nf.POSCheckNumber = cheque._Fld33142;
                    nf.AmountCash = cheque._Fld33144;
                    nf.AmountDoc = cheque._Fld33148;

                    if (nf.AmountDoc.Value > nf.AmountCash.Value)
                        nf.AmountPayCards = nf.AmountDoc.Value - nf.AmountCash.Value;
                    else
                    {
                        nf.AmountPayCards = 0;
                        nf.AmountCash = nf.AmountDoc.Value;
                    }

                    // Warehouses
                    if (fndWarehouses.ContainsKey(cheque._Fld33146RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(cheque._Fld33146RRef, out val);
                        nf.WarehouseID = val;
                    }
                    else
                        throw new Exception();

                    // Currencies
                    if (fndCurrencies.ContainsKey(cheque._Fld33136RRef))
                    {
                        Int64 val;
                        fndCurrencies.TryGetValue(cheque._Fld33136RRef, out val);
                        nf.CurrencyID = val;
                    }
                    else
                        throw new Exception();

                    // PriceTypes
                    if (fndPriceTypes.ContainsKey(cheque._Fld33137RRef))
                    {
                        Int64 val;
                        fndPriceTypes.TryGetValue(cheque._Fld33137RRef, out val);
                        nf.PriceTypeID = val;
                    }
                    else
                        throw new Exception();

                    // POS Registers
                    if (fndRegisters.ContainsKey(cheque._Fld33138RRef))
                    {
                        Int64 val;
                        fndRegisters.TryGetValue(cheque._Fld33138RRef, out val);
                        nf.POSRegisterID = val;
                    }
                    else
                        throw new Exception();

                    // Discount cards
                    if (fndCards.ContainsKey(cheque._Fld33153RRef))
                    {
                        string val;
                        fndCards.TryGetValue(cheque._Fld33153RRef, out val);
                        nf.DiscountCardNumber = val;
                    }

                    nf.Refund = false;

                    nf.Version = cheque._Version;

                    if (cheque._Posted == posted)
                        nf.Active = true;
                    else
                        nf.Active = false;

                    // update lines
                    var lines = from l in dataContext.FactPOSLines
                                where l.Document898_IDRRef == cheque._IDRRef
                                select l;
                    dataContext.FactPOSLines.DeleteAllOnSubmit(lines);
                    dataContext.SubmitChanges();

                    impFactPOSLines(cheque._IDRRef);

                }
                else
                {
                    // NEW
                    FactPOS nf = new FactPOS();
                    nf.NumberPrefix = cheque._NumberPrefix;
                    nf.ChequeNumber = cheque._Number;
                    nf.DocDate = cheque._Date_Time.AddYears(-2000);

                    // DateKey
                    if (fndDateKey.ContainsKey(nf.DocDate.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(nf.DocDate.Value.Date, out val);
                        nf.DateKey = val;
                    }
                    else
                        throw new Exception();

                    nf.Posted = cheque._Posted;

                    // People
                    if (fndPeople.ContainsKey(cheque._Fld33139RRef))
                    {
                        Int64 val;
                        fndPeople.TryGetValue(cheque._Fld33139RRef, out val);
                        nf.CasherID = val;
                    }
                    else
                        throw new Exception();

                    nf.POSCheckNumber = cheque._Fld33142;
                    nf.AmountCash = cheque._Fld33144;
                    nf.AmountDoc = cheque._Fld33148;

                    if (nf.AmountDoc.Value > nf.AmountCash.Value)
                        nf.AmountPayCards = nf.AmountDoc.Value - nf.AmountCash.Value;
                    else
                    {
                        nf.AmountPayCards = 0;
                        nf.AmountCash = nf.AmountDoc.Value;
                    }

                    // Warehouses
                    if (fndWarehouses.ContainsKey(cheque._Fld33146RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(cheque._Fld33146RRef, out val);
                        nf.WarehouseID = val;
                    }
                    else
                        throw new Exception();

                    // Currencies
                    if (fndCurrencies.ContainsKey(cheque._Fld33136RRef))
                    {
                        Int64 val;
                        fndCurrencies.TryGetValue(cheque._Fld33136RRef, out val);
                        nf.CurrencyID = val;
                    }
                    else
                        throw new Exception();

                    // PriceTypes
                    if (fndPriceTypes.ContainsKey(cheque._Fld33137RRef))
                    {
                        Int64 val;
                        fndPriceTypes.TryGetValue(cheque._Fld33137RRef, out val);
                        nf.PriceTypeID = val;
                    }
                    else
                        throw new Exception();

                    // POS Registers
                    if (fndRegisters.ContainsKey(cheque._Fld33138RRef))
                    {
                        Int64 val;
                        fndRegisters.TryGetValue(cheque._Fld33138RRef, out val);
                        nf.POSRegisterID = val;
                    }
                    else
                        throw new Exception();

                    // Discount cards
                    if (fndCards.ContainsKey(cheque._Fld33153RRef))
                    {
                        string val;
                        fndCards.TryGetValue(cheque._Fld33153RRef, out val);
                        nf.DiscountCardNumber = val;
                    }

                    nf.Refund = false;

                    nf.IDRRef = cheque._IDRRef;
                    nf.Version = cheque._Version;

                    if (cheque._Posted == posted)
                        nf.Active = true;
                    else
                        nf.Active = false;
                    dataContext.FactPOS.InsertOnSubmit(nf);
                    dataContext.SubmitChanges();

                    impFactPOSLines(cheque._IDRRef); // import lines

                } // if (fndDocs.Contains(cheque._IDRRef) == true)

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;

            } // foreach(_Document898 cheque in cheques)

            dataContext.SubmitChanges();

        } // public void impFactPOS()

        public void impFactPOSReturn()
        {
            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndDocs = new HashSet<System.Data.Linq.Binary>();
            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                            System.Data.Linq.Binary>> fndDocVer = new HashSet<Tuple<System.Data.Linq.Binary,
                            System.Data.Linq.Binary>>();

            Dictionary<System.Data.Linq.Binary, Int64> fndGoods = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndWarehouses = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndPeople = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndCurrencies = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndPriceTypes = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndRegisters = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, string> fndCards = new Dictionary<System.Data.Linq.Binary, string>();

            Dictionary<DateTime, int> fndDateKey = new Dictionary<DateTime, int>();

            // Fill documents
            var docs = from d in dataContext.FactPOS
                       select d;
            foreach (FactPOS doc in docs)
            {
                fndDocs.Add(doc.IDRRef);
            }
            foreach (FactPOS doc in docs)
            {
                fndDocVer.Add(Tuple.Create(doc.IDRRef, doc.Version));
            }
            // Fill documents

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

            // People
            var fPeople = from p in dataContext.DimPeople
                          select p;
            foreach (DimPeople dp in fPeople)
            {
                fndPeople.Add(dp.IDRRef, dp.ID);
            }
            // People

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

            // POS Registers
            var fRegisters = from g in dataContext.DimPOSRegisters
                             select g;
            foreach (DimPOSRegisters gd in fRegisters)
            {
                fndRegisters.Add(gd.IDRRef, gd.ID);
            }
            // POS Registers

            // Loyal cards
            var fCards = from c in dataContextS1._Reference177
                         select c;
            foreach (_Reference177 crd in fCards)
            {
                fndCards.Add(crd._IDRRef, crd._Description);
            }
            // Loyal cards

            // DateKey
            var fDateKey = from g in dataContext.DimDates
                           select g;
            foreach (DimDates gd in fDateKey)
            {
                fndDateKey.Add(gd.Date, gd.DateKey);
            }
            // DateKey

            // Marked deleted
            byte[] posted = new byte[1];
            posted[0] = 1;
            // Marked deleted

            var cheques = from c in dataContextS1._Document899
                          where c._Posted == posted
                          select c;

            int i = 0;
            foreach (_Document899 cheque in cheques)
            {

                if (fndDocVer.Contains(Tuple.Create(cheque._IDRRef, cheque._Version)) == true)
                {
                    i++;
                    continue;
                }

                if (fndDocs.Contains(cheque._IDRRef) == true)
                {
                    // UPD

                    var nf = (from n in dataContext.FactPOS
                              where n.IDRRef == cheque._IDRRef
                              select n).FirstOrDefault();
                    nf.NumberPrefix = cheque._NumberPrefix;
                    nf.ChequeNumber = cheque._Number;
                    nf.DocDate = cheque._Date_Time.AddYears(-2000);

                    // DateKey
                    if (fndDateKey.ContainsKey(nf.DocDate.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(nf.DocDate.Value.Date, out val);
                        nf.DateKey = val;
                    }
                    else
                        throw new Exception();

                    nf.Posted = cheque._Posted;

                    // People
                    if (fndPeople.ContainsKey(cheque._Fld33220RRef))
                    {
                        Int64 val;
                        fndPeople.TryGetValue(cheque._Fld33220RRef, out val);
                        nf.CasherID = val;
                    }
                    else
                        throw new Exception();

                    nf.POSCheckNumber = cheque._Fld33223;
                    nf.AmountCash = 0;
                    nf.AmountDoc = cheque._Fld33226 * (-1);
                    nf.AmountPayCards = 0;



                    // Warehouses
                    if (fndWarehouses.ContainsKey(cheque._Fld33225RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(cheque._Fld33225RRef, out val);
                        nf.WarehouseID = val;
                    }
                    else
                        throw new Exception();

                    // Currencies
                    if (fndCurrencies.ContainsKey(cheque._Fld33217RRef))
                    {
                        Int64 val;
                        fndCurrencies.TryGetValue(cheque._Fld33217RRef, out val);
                        nf.CurrencyID = val;
                    }
                    else
                        throw new Exception();

                    // PriceTypes
                    if (fndPriceTypes.ContainsKey(cheque._Fld33218RRef))
                    {
                        Int64 val;
                        fndPriceTypes.TryGetValue(cheque._Fld33218RRef, out val);
                        nf.PriceTypeID = val;
                    }
                    else
                        throw new Exception();

                    // POS Registers
                    if (fndRegisters.ContainsKey(cheque._Fld33219RRef))
                    {
                        Int64 val;
                        fndRegisters.TryGetValue(cheque._Fld33219RRef, out val);
                        nf.POSRegisterID = val;
                    }
                    else
                        throw new Exception();

                    // Discount cards
                    if (fndCards.ContainsKey(cheque._Fld33235RRef))
                    {
                        string val;
                        fndCards.TryGetValue(cheque._Fld33235RRef, out val);
                        nf.DiscountCardNumber = val;
                    }

                    nf.Refund = true;

                    nf.Version = cheque._Version;

                    if (cheque._Posted == posted)
                        nf.Active = true;
                    else
                        nf.Active = false;

                    // update lines
                    var lines = from l in dataContext.FactPOSLines
                                where l.Document898_IDRRef == cheque._IDRRef
                                select l;
                    dataContext.FactPOSLines.DeleteAllOnSubmit(lines);
                    dataContext.SubmitChanges();

                    impFactPosReturnLines(cheque._IDRRef);
                }
                else
                {
                    // NEW
                    FactPOS nf = new FactPOS();
                    nf.NumberPrefix = cheque._NumberPrefix;
                    nf.ChequeNumber = cheque._Number;
                    nf.DocDate = cheque._Date_Time.AddYears(-2000);

                    // DateKey
                    if (fndDateKey.ContainsKey(nf.DocDate.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(nf.DocDate.Value.Date, out val);
                        nf.DateKey = val;
                    }
                    else
                        throw new Exception();

                    nf.Posted = cheque._Posted;

                    // People
                    if (fndPeople.ContainsKey(cheque._Fld33220RRef))
                    {
                        Int64 val;
                        fndPeople.TryGetValue(cheque._Fld33220RRef, out val);
                        nf.CasherID = val;
                    }
                    else
                        throw new Exception();

                    nf.POSCheckNumber = cheque._Fld33223;
                    nf.AmountCash = 0;
                    nf.AmountDoc = cheque._Fld33226 * (-1);
                    nf.AmountPayCards = 0;

                    // Warehouses
                    if (fndWarehouses.ContainsKey(cheque._Fld33225RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(cheque._Fld33225RRef, out val);
                        nf.WarehouseID = val;
                    }
                    else
                        throw new Exception();

                    // Currencies
                    if (fndCurrencies.ContainsKey(cheque._Fld33217RRef))
                    {
                        Int64 val;
                        fndCurrencies.TryGetValue(cheque._Fld33217RRef, out val);
                        nf.CurrencyID = val;
                    }
                    else
                        throw new Exception();

                    // PriceTypes
                    if (fndPriceTypes.ContainsKey(cheque._Fld33218RRef))
                    {
                        Int64 val;
                        fndPriceTypes.TryGetValue(cheque._Fld33218RRef, out val);
                        nf.PriceTypeID = val;
                    }
                    else
                        throw new Exception();

                    // POS Registers
                    if (fndRegisters.ContainsKey(cheque._Fld33219RRef))
                    {
                        Int64 val;
                        fndRegisters.TryGetValue(cheque._Fld33219RRef, out val);
                        nf.POSRegisterID = val;
                    }
                    else
                        throw new Exception();

                    // Discount cards
                    if (fndCards.ContainsKey(cheque._Fld33235RRef))
                    {
                        string val;
                        fndCards.TryGetValue(cheque._Fld33235RRef, out val);
                        nf.DiscountCardNumber = val;
                    }

                    nf.Refund = true;

                    nf.IDRRef = cheque._IDRRef;
                    nf.Version = cheque._Version;

                    if (cheque._Posted == posted)
                        nf.Active = true;
                    else
                        nf.Active = false;
                    dataContext.FactPOS.InsertOnSubmit(nf);
                    dataContext.SubmitChanges();

                    impFactPosReturnLines(cheque._IDRRef); // import lines

                } // if (fndDocs.Contains(cheque._IDRRef) == true)

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;

            } // foreach(_Document898 cheque in cheques)

            dataContext.SubmitChanges();
        } // public void impFactPOSReturn()

        private void impFactPOSLines(System.Data.Linq.Binary docIDRRef)
        {
            var cheque = (from c in dataContext.FactPOS
                          where c.IDRRef == docIDRRef
                          select c).FirstOrDefault();

            var chequeLns = from c in dataContextS1._Document898_VT33156
                            where c._Document898_IDRRef == docIDRRef
                            select c;

            int i = 0;
            foreach (_Document898_VT33156 ln in chequeLns)
            {
                FactPOSLines nl = new FactPOSLines();
                nl.DocID = cheque.ID;

                // Goods
                if (fndGoods.ContainsKey(ln._Fld33159RRef))
                {
                    Int64 val;
                    fndGoods.TryGetValue(ln._Fld33159RRef, out val);
                    nl.GoodID = val;
                }
                else
                    throw new Exception();

                nl.Qty = ln._Fld33163;

                // Units
                var edIzmBId = (from r in edIzmerId
                                where r.Key == nl.GoodID.Value
                                select r).FirstOrDefault();

                nl.BaseUnitID = edIzmBId.Value;

                nl.QtyPcs = ln._Fld49975;
                nl.Price = ln._Fld33164;
                nl.Amount = ln._Fld33165;
                nl.AmountTax = ln._Fld33167;
                nl.DiscountAutoPercent = ln._Fld33168;
                nl.DiscountAutoAmount = ln._Fld33169;
                nl.DiscountManualPercent = ln._Fld33170;
                nl.DiscountManualAmount = ln._Fld33171;

                // People
                if (fndPeople.ContainsKey(ln._Fld33173RRef))
                {
                    Int64 val;
                    fndPeople.TryGetValue(ln._Fld33173RRef, out val);
                    nl.CasherID = val;
                }
                else
                    throw new Exception();

                nl.Barcode = ln._Fld49747;
                nl.Document898_IDRRef = ln._Document898_IDRRef;
                nl.KeyField = ln._KeyField;
                nl.LineNum = ln._LineNo33157;
                nl.Refund = false;
                nl.Active = true;
                dataContext.FactPOSLines.InsertOnSubmit(nl);

            } // foreach (_Document898_VT33156 ln in chequeLns)

        } // impFactPOSLines(System.Data.Linq.Binary docIDRRef)

        private void impFactPosReturnLines(System.Data.Linq.Binary docIDRRef)
        {
            var cheque = (from c in dataContext.FactPOS
                          where c.IDRRef == docIDRRef
                          select c).FirstOrDefault();

            var chequeLns = from c in dataContextS1._Document899_VT33236
                            where c._Document899_IDRRef == docIDRRef
                            select c;

            int i = 0;
            foreach (_Document899_VT33236 ln in chequeLns)
            {
                FactPOSLines nl = new FactPOSLines();
                nl.DocID = cheque.ID;

                // Goods
                if (fndGoods.ContainsKey(ln._Fld33238RRef))
                {
                    Int64 val;
                    fndGoods.TryGetValue(ln._Fld33238RRef, out val);
                    nl.GoodID = val;
                }
                else
                    throw new Exception();

                nl.Qty = ln._Fld33242;

                // Units
                var edIzmBId = (from r in edIzmerId
                                where r.Key == nl.GoodID.Value
                                select r).FirstOrDefault();

                nl.BaseUnitID = edIzmBId.Value;

                nl.QtyPcs = ln._Fld49976 * (-1);
                nl.Price = ln._Fld33243;
                nl.Amount = ln._Fld33244 * (-1);
                nl.AmountTax = ln._Fld33246 * (-1);
                nl.DiscountAutoPercent = 0;
                nl.DiscountAutoAmount = 0;
                nl.DiscountManualPercent = 0;
                nl.DiscountManualAmount = 0;

                // People
                if (fndPeople.ContainsKey(ln._Fld33248RRef))
                {
                    Int64 val;
                    fndPeople.TryGetValue(ln._Fld33248RRef, out val);
                    nl.CasherID = val;
                }
                else
                    throw new Exception();

                nl.Barcode = ln._Fld49757;
                nl.Document898_IDRRef = ln._Document899_IDRRef;
                nl.KeyField = ln._KeyField;
                nl.LineNum = ln._LineNo33237;
                nl.Refund = true;
                nl.Active = true;
                dataContext.FactPOSLines.InsertOnSubmit(nl);

            } // foreach (_Document898_VT33156 ln in chequeLns)
        } // private void impFactPosReturnLines(System.Data.Linq.Binary docIDRRef)

        // Del
        // current Remains import by warehouse deleted
        public void impFactPOSDel()
        {
            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> loadPOSHist = new HashSet<System.Data.Linq.Binary>();

            // Marked deleted
            byte[] posted = new byte[1];
            posted[0] = 1;
            // Marked deleted

            // From 1C 
            var posDocs = from t in dataContextS1._Document898
                          where t._Posted == posted
                          select t;

            foreach (_Document898 cheque in posDocs)
            {
                loadPOSHist.Add(cheque._IDRRef);
            }
            // From 1C tovary na skladah

            var fPOSHist = from f in dataContext.FactPOS
                           select f;

            int i = 0;
            foreach (FactPOS rh in fPOSHist)
            {

                if (loadPOSHist.Contains(rh.IDRRef) == true)
                {
                    i++;
                    continue;
                }

                //rh.Active = false;
                impFactPOSLinesDel(rh.IDRRef);
                dataContext.FactPOS.DeleteOnSubmit(rh);

                if (i % 50 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        } // public void impFactPOSDel()

        private void impFactPOSLinesDel(System.Data.Linq.Binary docIDRRef)
        {
            var chequeLns = from c in dataContext.FactPOSLines
                            where c.Document898_IDRRef == docIDRRef
                            select c;

            dataContext.FactPOSLines.DeleteAllOnSubmit(chequeLns);
        }

        public void impFactPOSReturnDel()
        {
            throw new NotImplementedException();
        }

    } // public class FactPOSClass
}