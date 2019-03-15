using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ERPReportUtils
{
    public class DocProductionImporter
    {
        DataClasses1DataContext dataContext;
        s1_DataClasses1DataContext dataContextS1;

        Dictionary<System.Data.Linq.Binary, Int64> fndGoods = new Dictionary<System.Data.Linq.Binary, Int64>();
        Dictionary<Int64, Int64?> edIzmerId = new Dictionary<Int64, Int64?>();
        Dictionary<System.Data.Linq.Binary, Int64> fndSpecs = new Dictionary<System.Data.Linq.Binary, Int64>();

        Dictionary<System.Data.Linq.Binary, Int64> fndWarehouses = new Dictionary<System.Data.Linq.Binary, Int64>();
        Dictionary<System.Data.Linq.Binary, Int64> fndDepartments = new Dictionary<System.Data.Linq.Binary, Int64>();
        Dictionary<System.Data.Linq.Binary, Int64> fndCurrencies = new Dictionary<System.Data.Linq.Binary, Int64>();
        Dictionary<System.Data.Linq.Binary, Int64> fndPriceTypes = new Dictionary<System.Data.Linq.Binary, Int64>();
        Dictionary<System.Data.Linq.Binary, Int64> fndPeople = new Dictionary<System.Data.Linq.Binary, Int64>();
        Dictionary<System.Data.Linq.Binary, Int64> fndTechOperation = new Dictionary<System.Data.Linq.Binary, Int64>();

        Dictionary<DateTime, int> fndDateKey = new Dictionary<DateTime, int>();

        Int64 unitIDGramm = 18; // ID for unit Gramm

        public DocProductionImporter()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();
        }

        public void impDocProduction()
        {

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndDocs = new HashSet<System.Data.Linq.Binary>();
            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                            System.Data.Linq.Binary>> fndDocVer = new HashSet<Tuple<System.Data.Linq.Binary,
                            System.Data.Linq.Binary>>();


            // Fill documents
            var docs = from d in dataContext.DocProductions
                       select d;
            foreach (DocProduction doc in docs)
            {
                fndDocs.Add(doc.IDRRef);
            }
            foreach (DocProduction doc in docs)
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

            // Departments
            var fDepartments = from g in dataContext.DimDepartments
                               select g;
            foreach (DimDepartments fd in fDepartments)
            {
                fndDepartments.Add(fd.IDRRef, fd.ID);
            }
            // Departments

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
            // Specifications
            var fSpecs = from g in dataContext.DimSpecifications
                         select g;
            foreach (DimSpecifications gd in fSpecs)
            {
                fndSpecs.Add(gd.IDRRef, gd.ID);
            }
            // Specifications
            // Tech operations
            var fOps = from g in dataContext.DimTechOperations
                         select g;
            foreach (DimTechOperation gd in fOps)
            {
                fndTechOperation.Add(gd.IDRRef, gd.ID);
            }
            // Tech operations

            // Marked deleted
            byte[] posted = new byte[1];
            posted[0] = 1;
            // Marked deleted


            var prodDocs = from c in dataContextS1._Document581s
                           where c._Posted == posted
                           select c;
            int i = 0;
            foreach (_Document581 doc in prodDocs)
            {

                if (fndDocVer.Contains(Tuple.Create(doc._IDRRef, doc._Version)) == true)
                {
                    i++;
                    continue;
                }

                if (fndDocs.Contains(doc._IDRRef) == true)
                {
                    // UPD
                    var dp = (from n in dataContext.DocProductions
                              where n.IDRRef == doc._IDRRef
                              select n).FirstOrDefault();

                    dp.NumberPrefix = doc._NumberPrefix;
                    dp.DocNumber = doc._Number;

                    // Warehouses
                    if (fndWarehouses.ContainsKey(doc._Fld15868RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(doc._Fld15868RRef, out val);
                        dp.WarehouseID = val;
                    }
                    else
                        throw new Exception();

                    // Department
                    if (fndDepartments.ContainsKey(doc._Fld15867RRef))
                    {
                        Int64 val;
                        fndDepartments.TryGetValue(doc._Fld15867RRef, out val);
                        dp.DepartmentID = val;
                    }
                    else
                        throw new Exception();

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

                    dp.ConsignNumber = doc._Fld49981;
                    dp.Posted = doc._Posted;

                    // PriceTypes
                    if (fndPriceTypes.ContainsKey(doc._Fld15869RRef))
                    {
                        Int64 val;
                        fndPriceTypes.TryGetValue(doc._Fld15869RRef, out val);
                        dp.PriceTypeID = val;
                    }
                    //else
                    //    throw new Exception();

                    // People
                    if (fndPeople.ContainsKey(doc._Fld15870RRef))
                    {
                        Int64 val;
                        fndPeople.TryGetValue(doc._Fld15870RRef, out val);
                        dp.UserID = val;
                    }
                    else
                        throw new Exception();

                    // Currencies
                    if (fndCurrencies.ContainsKey(doc._Fld15875RRef))
                    {
                        Int64 val;
                        fndCurrencies.TryGetValue(doc._Fld15875RRef, out val);
                        dp.CurrencyID = val;
                    }
                    else
                        throw new Exception();

                    // Tech operations
                    if (fndTechOperation.ContainsKey(doc._Fld50221RRef))
                    {
                        Int64 val;
                        fndTechOperation.TryGetValue(doc._Fld50221RRef, out val);
                        dp.TechOperationID = val;
                    }
                    //else
                    //    throw new Exception();

                    dp.Version = doc._Version;

                    if (doc._Posted == posted)
                        dp.Active = true;
                    else
                        dp.Active = false;
                    dataContext.SubmitChanges();

                    // update lines
                    var lines = from l in dataContext.DocProductionLines
                                where l.Document581_IDRRef == doc._IDRRef
                                select l;
                    dataContext.DocProductionLines.DeleteAllOnSubmit(lines);
                    dataContext.SubmitChanges();
                    impDocProductionLines(doc._IDRRef, dp.DepartmentID); // import lines
                }
                else
                {
                    // NEW
                    DocProduction dp = new DocProduction();
                    dp.NumberPrefix = doc._NumberPrefix;
                    dp.DocNumber = doc._Number;

                    // Warehouses
                    if (fndWarehouses.ContainsKey(doc._Fld15868RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(doc._Fld15868RRef, out val);
                        dp.WarehouseID = val;
                    }
                    else
                        throw new Exception();

                    // Department
                    if (fndDepartments.ContainsKey(doc._Fld15867RRef))
                    {
                        Int64 val;
                        fndDepartments.TryGetValue(doc._Fld15867RRef, out val);
                        dp.DepartmentID = val;
                    }
                    else
                        throw new Exception();

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

                    dp.ConsignNumber = doc._Fld49981;
                    dp.Posted = doc._Posted;

                    // PriceTypes
                    if (fndPriceTypes.ContainsKey(doc._Fld15869RRef))
                    {
                        Int64 val;
                        fndPriceTypes.TryGetValue(doc._Fld15869RRef, out val);
                        dp.PriceTypeID = val;
                    }
                    //else
                    //    throw new Exception();

                    // People
                    if (fndPeople.ContainsKey(doc._Fld15870RRef))
                    {
                        Int64 val;
                        fndPeople.TryGetValue(doc._Fld15870RRef, out val);
                        dp.UserID = val;
                    }
                    else
                        throw new Exception();

                    // Currencies
                    if (fndCurrencies.ContainsKey(doc._Fld15875RRef))
                    {
                        Int64 val;
                        fndCurrencies.TryGetValue(doc._Fld15875RRef, out val);
                        dp.CurrencyID = val;
                    }
                    else
                        throw new Exception();

                    // Tech operations
                    if (fndTechOperation.ContainsKey(doc._Fld50221RRef))
                    {
                        Int64 val;
                        fndTechOperation.TryGetValue(doc._Fld50221RRef, out val);
                        dp.TechOperationID = val;
                    }
                    //else
                    //    throw new Exception();

                    dp.IDRRef = doc._IDRRef;
                    dp.Version = doc._Version;

                    if (doc._Posted == posted)
                        dp.Active = true;
                    else
                        dp.Active = false;
                    dataContext.DocProductions.InsertOnSubmit(dp);
                    dataContext.SubmitChanges();

                    impDocProductionLines(doc._IDRRef, dp.DepartmentID); // import lines

                } // if (fndDocs.Contains(doc._IDRRef) == true)

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;

            } // foreach (_Document581 doc in prodDocs)

            dataContext.SubmitChanges();

        } // public void impDocProduction()

        public void impDocProductionDel()
        {
            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> loadDocHist = new HashSet<System.Data.Linq.Binary>();

            // Marked deleted
            byte[] posted = new byte[1];
            posted[0] = 1;
            // Marked deleted

            // From 1C 
            var posDocs = from t in dataContextS1._Document581s
                          where t._Posted == posted
                          select t;

            foreach (_Document581 cheque in posDocs)
            {
                loadDocHist.Add(cheque._IDRRef);
            }
            // From 1C tovary na skladah

            var fDocHists = from f in dataContext.DocProductions
                            select f;

            int i = 0;
            foreach (DocProduction dc in fDocHists)
            {

                if (loadDocHist.Contains(dc.IDRRef) == true)
                {
                    i++;
                    continue;
                }

                //dc.Active = false;
                impDocProductionLinesDel(dc.IDRRef);
                dataContext.DocProductions.DeleteOnSubmit(dc);

                if (i % 50 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        } // public void impDocProductionDel()

        private void impDocProductionLinesDel(System.Data.Linq.Binary docIDRRef)
        {
            var dLns = from c in dataContext.DocProductionLines
                       where c.Document581_IDRRef == docIDRRef
                       select c;

            dataContext.DocProductionLines.DeleteAllOnSubmit(dLns);
        }

        private void impDocProductionLines(System.Data.Linq.Binary docIDRRef, Int64? docDepartmentID)
        {

            var doc = (from c in dataContext.DocProductions
                       where c.IDRRef == docIDRRef
                       select c).FirstOrDefault();

            var docLns = from c in dataContextS1._Document581_VT15877s
                         where c._Document581_IDRRef == docIDRRef
                         select c;

            int i = 0;
            foreach (_Document581_VT15877 ln in docLns)
            {
                DocProductionLines nl = new DocProductionLines();
                nl.DocID = doc.ID;

                // Goods
                if (fndGoods.ContainsKey(ln._Fld15879RRef))
                {
                    Int64 val;
                    fndGoods.TryGetValue(ln._Fld15879RRef, out val);
                    nl.GoodID = val;
                }
                else
                    throw new Exception();

                nl.BaseQty = ln._Fld15884;

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

                nl.QtyPcs = ln._Fld45651;
                nl.Price = ln._Fld15885;
                nl.Amount = ln._Fld15886;

                // Spec
                if (fndSpecs.ContainsKey(ln._Fld15881RRef))
                {
                    Int64 val;
                    fndSpecs.TryGetValue(ln._Fld15881RRef, out val);
                    nl.SpecificationID = val;
                }
                //else
                //    throw new Exception();

                // Warehouses
                if (fndWarehouses.ContainsKey(ln._Fld15900RRef))
                {
                    Int64 val;
                    fndWarehouses.TryGetValue(ln._Fld15900RRef, out val);
                    nl.WarehouseID = val;
                }
                else
                    throw new Exception();

                // Department
                if (fndDepartments.ContainsKey(ln._Fld15901RRef))
                {
                    Int64 val;
                    fndDepartments.TryGetValue(ln._Fld15901RRef, out val);
                    nl.DepartmentID = val;
                }
                else
                {
                    nl.DepartmentID = docDepartmentID;
                }

                nl.Document581_IDRRef = ln._Document581_IDRRef;
                nl.KeyField = ln._KeyField;
                nl.LineNum = ln._LineNo15878;
                nl.Active = true;
                dataContext.DocProductionLines.InsertOnSubmit(nl);

            } // foreach (_Document581_VT15877 ln in docLns)

        } // private void impDocProductionLines(System.Data.Linq.Binary docIDRRef)

        // Costs:

        public void impDocProductionCosts()
        {

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndDocs = new HashSet<System.Data.Linq.Binary>();
            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                            System.Data.Linq.Binary>> fndDocVer = new HashSet<Tuple<System.Data.Linq.Binary,
                            System.Data.Linq.Binary>>();

            // Fill documents
            var docs = from d in dataContext.DocProductionCosts
                       select d;
            foreach (DocProductionCosts doc in docs)
            {
                fndDocs.Add(doc.IDRRef);
            }
            foreach (DocProductionCosts doc in docs)
            {
                fndDocVer.Add(Tuple.Create(doc.IDRRef, doc.Version));
            }
            // Fill documents

            // Marked deleted
            byte[] posted = new byte[1];
            posted[0] = 1;
            // Marked deleted

            var prodDocs = from c in dataContextS1._Document846
                           where c._Posted == posted
                           select c;

            int i = 0;
            foreach (_Document846 doc in prodDocs)
            {

                if (fndDocVer.Contains(Tuple.Create(doc._IDRRef, doc._Version)) == true)
                {
                    i++;
                    continue;
                }

                if (fndDocs.Contains(doc._IDRRef) == true)
                {
                    // UPD
                    var dp = (from n in dataContext.DocProductionCosts
                              where n.IDRRef == doc._IDRRef
                              select n).FirstOrDefault();
                    dp.NumberPrefix = doc._NumberPrefix;
                    dp.DocNumber = doc._Number;
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

                    dp.ConsignNumber = doc._Fld49990;
                    dp.Posted = doc._Posted;

                    // Department
                    if (fndDepartments.ContainsKey(doc._Fld30554RRef))
                    {
                        Int64 val;
                        fndDepartments.TryGetValue(doc._Fld30554RRef, out val);
                        dp.DepartmentID = val;
                    }
                    else
                        throw new Exception();

                    // Goods
                    if (fndGoods.ContainsKey(doc._Fld30541RRef))
                    {
                        Int64 val;
                        fndGoods.TryGetValue(doc._Fld30541RRef, out val);
                        dp.MasterGoodID = val;
                    }
                    else
                        throw new Exception();

                    dp.BaseQty = doc._Fld30551;
                    // Units
                    var edIzmBId = (from r in edIzmerId
                                    where r.Key == dp.MasterGoodID.Value
                                    select r).FirstOrDefault();

                    dp.BaseUnitID = edIzmBId.Value;

                    if (dp.BaseUnitID != unitIDGramm)
                    {
                        var unitConvs = (from u in dataContext.DimUnitConversion
                                         where u.FromUnitID == dp.BaseUnitID
                                         && u.ToUnitID == unitIDGramm
                                         select u).FirstOrDefault();
                        if (unitConvs == null)
                            dp.Qty = Math.Round(dp.BaseQty.Value, 3);
                        else
                            dp.Qty = Math.Round((unitConvs.ToQty.Value * dp.BaseQty.Value), 3);
                    }
                    else
                    {
                        dp.Qty = Math.Round(dp.BaseQty.Value, 3);
                    }
                    dp.UnitID = unitIDGramm;

                    // Spec
                    if (fndSpecs.ContainsKey(doc._Fld30544RRef))
                    {
                        Int64 val;
                        fndSpecs.TryGetValue(doc._Fld30544RRef, out val);
                        dp.SpecificationID = val;
                    }
                    //else
                    //    throw new Exception();

                    // People
                    if (fndPeople.ContainsKey(doc._Fld30547RRef))
                    {
                        Int64 val;
                        fndPeople.TryGetValue(doc._Fld30547RRef, out val);
                        dp.UserID = val;
                    }
                    else
                        throw new Exception();

                    dp.Version = doc._Version;

                    if (doc._Posted == posted)
                        dp.Active = true;
                    else
                        dp.Active = false;

                    dataContext.SubmitChanges();

                    // update lines
                    var linesOut = from l in dataContext.DocProductionCostOut
                                   where l.Document846_IDRRef == doc._IDRRef
                                   select l;
                    dataContext.DocProductionCostOut.DeleteAllOnSubmit(linesOut);
                    var linesIn = from l in dataContext.DocProductionCostIn
                                  where l.Document846_IDRRef == doc._IDRRef
                                  select l;
                    dataContext.DocProductionCostIn.DeleteAllOnSubmit(linesIn);
                    var linesMat = from l in dataContext.DocProductionCostMaterials
                                   where l.Document846_IDRRef == doc._IDRRef
                                   select l;
                    dataContext.DocProductionCostMaterials.DeleteAllOnSubmit(linesMat);
                    dataContext.SubmitChanges();

                    impDocProductionCostOutLines(doc._IDRRef); // import lines
                    impDocProductionCostInLines(doc._IDRRef);
                    impDocProductionCostMaterialsLines(doc._IDRRef);

                }
                else
                {
                    // NEW
                    DocProductionCosts dp = new DocProductionCosts();
                    dp.NumberPrefix = doc._NumberPrefix;
                    dp.DocNumber = doc._Number;
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

                    dp.ConsignNumber = doc._Fld49990;
                    dp.Posted = doc._Posted;

                    // Department
                    if (fndDepartments.ContainsKey(doc._Fld30554RRef))
                    {
                        Int64 val;
                        fndDepartments.TryGetValue(doc._Fld30554RRef, out val);
                        dp.DepartmentID = val;
                    }
                    else
                        throw new Exception();

                    // Goods
                    if (fndGoods.ContainsKey(doc._Fld30541RRef))
                    {
                        Int64 val;
                        fndGoods.TryGetValue(doc._Fld30541RRef, out val);
                        dp.MasterGoodID = val;
                    }
                    else
                        throw new Exception();

                    dp.BaseQty = doc._Fld30551;
                    // Units
                    var edIzmBId = (from r in edIzmerId
                                    where r.Key == dp.MasterGoodID.Value
                                    select r).FirstOrDefault();

                    dp.BaseUnitID = edIzmBId.Value;

                    if (dp.BaseUnitID != unitIDGramm)
                    {
                        var unitConvs = (from u in dataContext.DimUnitConversion
                                         where u.FromUnitID == dp.BaseUnitID
                                         && u.ToUnitID == unitIDGramm
                                         select u).FirstOrDefault();
                        if (unitConvs == null)
                            dp.Qty = Math.Round(dp.BaseQty.Value, 3);
                        else
                            dp.Qty = Math.Round((unitConvs.ToQty.Value * dp.BaseQty.Value), 3);
                    }
                    else
                    {
                        dp.Qty = Math.Round(dp.BaseQty.Value, 3);
                    }
                    dp.UnitID = unitIDGramm;

                    // Spec
                    if (fndSpecs.ContainsKey(doc._Fld30544RRef))
                    {
                        Int64 val;
                        fndSpecs.TryGetValue(doc._Fld30544RRef, out val);
                        dp.SpecificationID = val;
                    }
                    //else
                    //    throw new Exception();

                    // People
                    if (fndPeople.ContainsKey(doc._Fld30547RRef))
                    {
                        Int64 val;
                        fndPeople.TryGetValue(doc._Fld30547RRef, out val);
                        dp.UserID = val;
                    }
                    else
                        throw new Exception();

                    dp.IDRRef = doc._IDRRef;
                    dp.Version = doc._Version;

                    if (doc._Posted == posted)
                        dp.Active = true;
                    else
                        dp.Active = false;
                    dataContext.DocProductionCosts.InsertOnSubmit(dp);
                    dataContext.SubmitChanges();

                    impDocProductionCostOutLines(doc._IDRRef); // import lines
                    impDocProductionCostInLines(doc._IDRRef);
                    impDocProductionCostMaterialsLines(doc._IDRRef);

                } // if (fndDocs.Contains(doc._IDRRef) == true)

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;


            } // foreach (_Document846 doc in prodDocs)
            dataContext.SubmitChanges();

        } // public void impDocProductionCosts()

        private void impDocProductionCostOutLines(System.Data.Linq.Binary docIDRRef)
        {

            var doc = (from c in dataContext.DocProductionCosts
                       where c.IDRRef == docIDRRef
                       select c).FirstOrDefault();

            var docLns = from c in dataContextS1._Document846_VT30555
                         where c._Document846_IDRRef == docIDRRef
                         select c;

            int i = 0;
            foreach (_Document846_VT30555 ln in docLns)
            {
                DocProductionCostOut nl = new DocProductionCostOut();
                nl.DocID = doc.ID;

                // Goods
                if (fndGoods.ContainsKey(ln._Fld30557RRef))
                {
                    Int64 val;
                    fndGoods.TryGetValue(ln._Fld30557RRef, out val);
                    nl.GoodID = val;
                }
                else
                    throw new Exception();

                nl.BaseQty = ln._Fld30560;
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

                nl.QtyPcs = ln._Fld49971;
                nl.CostShare = ln._Fld30562;

                // Department
                if (fndDepartments.ContainsKey(ln._Fld30565RRef))
                {
                    Int64 val;
                    fndDepartments.TryGetValue(ln._Fld30565RRef, out val);
                    nl.DepartmentID = val;
                }
                else
                    throw new Exception();

                nl.Document846_IDRRef = ln._Document846_IDRRef;
                nl.KeyField = ln._KeyField;
                nl.LineNum = ln._LineNo30556;
                nl.Active = true;
                dataContext.DocProductionCostOut.InsertOnSubmit(nl);

            } // foreach (_Document846_VT30555 ln in docLns)
        }

        private void impDocProductionCostInLines(System.Data.Linq.Binary docIDRRef)
        {

            var doc = (from c in dataContext.DocProductionCosts
                       where c.IDRRef == docIDRRef
                       select c).FirstOrDefault();

            var docLns = from c in dataContextS1._Document846_VT30568
                         where c._Document846_IDRRef == docIDRRef
                         select c;

            int i = 0;
            foreach (_Document846_VT30568 ln in docLns)
            {
                DocProductionCostIn nl = new DocProductionCostIn();
                nl.DocID = doc.ID;

                // Goods
                if (fndGoods.ContainsKey(ln._Fld30570RRef))
                {
                    Int64 val;
                    fndGoods.TryGetValue(ln._Fld30570RRef, out val);
                    nl.GoodID = val;
                }
                else
                    throw new Exception();

                nl.BaseQty = ln._Fld30573;

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

                nl.QtyPcs = ln._Fld49972;

                // Department
                if (fndDepartments.ContainsKey(ln._Fld30578RRef))
                {
                    Int64 val;
                    fndDepartments.TryGetValue(ln._Fld30578RRef, out val);
                    nl.DepartmentID = val;
                }
                else
                    throw new Exception();

                nl.Document846_IDRRef = ln._Document846_IDRRef;
                nl.KeyField = ln._KeyField;
                nl.LineNum = ln._LineNo30569;
                nl.Active = true;
                dataContext.DocProductionCostIn.InsertOnSubmit(nl);

            } // foreach (_Document846_VT30555 ln in docLns)
        }

        private void impDocProductionCostMaterialsLines(System.Data.Linq.Binary docIDRRef)
        {

            var doc = (from c in dataContext.DocProductionCosts
                       where c.IDRRef == docIDRRef
                       select c).FirstOrDefault();

            var docLns = from c in dataContextS1._Document846_VT30582
                         where c._Document846_IDRRef == docIDRRef
                         select c;

            int i = 0;
            foreach (_Document846_VT30582 ln in docLns)
            {
                DocProductionCostMaterials nl = new DocProductionCostMaterials();
                nl.DocID = doc.ID;

                // Goods
                if (fndGoods.ContainsKey(ln._Fld30584RRef))
                {
                    Int64 val;
                    fndGoods.TryGetValue(ln._Fld30584RRef, out val);
                    nl.GoodID = val;
                }
                else
                    throw new Exception();

                nl.BaseQty = ln._Fld30587;

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

                nl.QtyPcs = ln._Fld49973;

                // Department
                if (fndDepartments.ContainsKey(ln._Fld30594RRef))
                {
                    Int64 val;
                    fndDepartments.TryGetValue(ln._Fld30594RRef, out val);
                    nl.DepartmentID = val;
                }
                else
                    throw new Exception();

                nl.Document846_IDRRef = ln._Document846_IDRRef;
                nl.KeyField = ln._KeyField;
                nl.LineNum = ln._LineNo30583;
                nl.Active = true;
                dataContext.DocProductionCostMaterials.InsertOnSubmit(nl);

            } // foreach (_Document846_VT30555 ln in docLns)
        }

        public void impDocProductionCostsDel()
        {
            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> loadDocHist = new HashSet<System.Data.Linq.Binary>();

            // Marked deleted
            byte[] posted = new byte[1];
            posted[0] = 1;
            // Marked deleted

            // From 1C 
            var posDocs = from t in dataContextS1._Document846
                          where t._Posted == posted
                          select t;

            foreach (_Document846 doc in posDocs)
            {
                loadDocHist.Add(doc._IDRRef);
            }
            // From 1C tovary na skladah

            var fDocHists = from f in dataContext.DocProductionCosts
                            select f;

            int i = 0;
            foreach (DocProductionCosts dc in fDocHists)
            {

                if (loadDocHist.Contains(dc.IDRRef) == true)
                {
                    i++;
                    continue;
                }

                //dc.Active = false;
                impDocProductionCostsLinesDel(dc.IDRRef);
                dataContext.DocProductionCosts.DeleteOnSubmit(dc);

                if (i % 50 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();
        } // public void impDocProductionCostsDel()

        private void impDocProductionCostsLinesDel(System.Data.Linq.Binary docIDRRef)
        {
            var dLns = from c in dataContext.DocProductionCostOut
                       where c.Document846_IDRRef == docIDRRef
                       select c;
            dataContext.DocProductionCostOut.DeleteAllOnSubmit(dLns);

            var dLnsIn = from c in dataContext.DocProductionCostIn
                         where c.Document846_IDRRef == docIDRRef
                         select c;
            dataContext.DocProductionCostIn.DeleteAllOnSubmit(dLnsIn);

            var dLnsMat = from c in dataContext.DocProductionCostMaterials
                          where c.Document846_IDRRef == docIDRRef
                          select c;
            dataContext.DocProductionCostMaterials.DeleteAllOnSubmit(dLnsMat);
        }

    } // public class DocProductionImporter

} // namespace ERPReportUtils
