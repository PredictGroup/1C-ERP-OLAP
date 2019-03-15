using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace ERPReportUtils
{
    public class FactManufactureDocs
    {
        DataClasses1DataContext dataContext;
        s1_DataClasses1DataContext dataContextS1;

        Dictionary<System.Data.Linq.Binary, Int64> fndGoods = new Dictionary<System.Data.Linq.Binary, Int64>();
        Dictionary<Int64, Int64?> edIzmerId = new Dictionary<Int64, Int64?>();

        Int64 unitIDGramm = 18; // ID for unit Gramm

        public FactManufactureDocs()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();
        }

        // import transfers documents to production departments
        public void impFactTransferToProduction()
        {

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndDocs = new HashSet<System.Data.Linq.Binary>();
            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                            System.Data.Linq.Binary>> fndDocVer = new HashSet<Tuple<System.Data.Linq.Binary,
                            System.Data.Linq.Binary>>();


            Dictionary<System.Data.Linq.Binary, Int64> fndWarehouses = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndDepartments = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndTechOperation = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<DateTime, int> fndDateKey = new Dictionary<DateTime, int>();

            // Fill documents
            var docs = from d in dataContext.DocTransferProductions
                       where d.TransferType == true
                       select d;
            foreach (DocTransferProduction doc in docs)
            {
                fndDocs.Add(doc.IDRRef);
            }
            foreach (DocTransferProduction doc in docs)
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

            // Departments
            var fDepartments = from g in dataContext.DimDepartments
                               select g;
            foreach (DimDepartments fd in fDepartments)
            {
                fndDepartments.Add(fd.IDRRef, fd.ID);
            }
            // Departments

            // Tech operations
            var fOps = from g in dataContext.DimTechOperations
                       select g;
            foreach (DimTechOperation gd in fOps)
            {
                fndTechOperation.Add(gd.IDRRef, gd.ID);
            }
            // Tech operations

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


            var docus = from c in dataContextS1._Document741s
                        where c._Posted == posted
                        select c;

            int i = 0;
            foreach (_Document741 docu in docus)
            {

                if (fndDocVer.Contains(Tuple.Create(docu._IDRRef, docu._Version)) == true)
                {
                    i++;
                    continue;
                }

                if (fndDocs.Contains(docu._IDRRef) == true)
                {
                    // UPD
                    var nf = (from n in dataContext.DocTransferProductions
                              where n.IDRRef == docu._IDRRef
                              && n.TransferType == true
                              select n).FirstOrDefault();

                    nf.TransferType = true; // +

                    nf.DocNumber = docu._Number;
                    nf.DocDate = docu._Date_Time.AddYears(-2000);

                    // DateKey
                    if (fndDateKey.ContainsKey(nf.DocDate.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(nf.DocDate.Value.Date, out val);
                        nf.DateKey = val;
                    }
                    else
                        throw new Exception();

                    nf.Posted = docu._Posted;

                    // Warehouses
                    if (fndWarehouses.ContainsKey(docu._Fld25261RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(docu._Fld25261RRef, out val);
                        nf.WarehouseID = val;
                    }
                    else
                        throw new Exception();

                    // Department
                    if (fndDepartments.ContainsKey(docu._Fld25263RRef))
                    {
                        Int64 val;
                        fndDepartments.TryGetValue(docu._Fld25263RRef, out val);
                        nf.DepartmentID = val;
                    }
                    else
                        throw new Exception();

                    nf.ConsignNumber = docu._Fld49984;

                    // Tech operations
                    if (fndTechOperation.ContainsKey(docu._Fld50224RRef))
                    {
                        Int64 val;
                        fndTechOperation.TryGetValue(docu._Fld50224RRef, out val);
                        nf.TechOperationID = val;
                    }
                    //else
                    //    throw new Exception();

                    nf.IDRRef = docu._IDRRef;
                    nf.Version = docu._Version;

                    if (docu._Posted == posted)
                        nf.Active = true;
                    else
                        nf.Active = false;

                    // update lines
                    var lines = from l in dataContext.DocTransferProductionLines
                                where l.Document741_IDRRef == docu._IDRRef
                                && l.TransferType == true
                                select l;
                    dataContext.DocTransferProductionLines.DeleteAllOnSubmit(lines);
                    dataContext.SubmitChanges();

                    impFactTransferToProductionLines(docu._IDRRef);

                }
                else
                {
                    // NEW
                    DocTransferProduction nf = new DocTransferProduction();

                    nf.TransferType = true; // +

                    nf.DocNumber = docu._Number;
                    nf.DocDate = docu._Date_Time.AddYears(-2000);

                    // DateKey
                    if (fndDateKey.ContainsKey(nf.DocDate.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(nf.DocDate.Value.Date, out val);
                        nf.DateKey = val;
                    }
                    else
                        throw new Exception();

                    nf.Posted = docu._Posted;

                    // Warehouses
                    if (fndWarehouses.ContainsKey(docu._Fld25261RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(docu._Fld25261RRef, out val);
                        nf.WarehouseID = val;
                    }
                    else
                        throw new Exception();

                    // Department
                    if (fndDepartments.ContainsKey(docu._Fld25263RRef))
                    {
                        Int64 val;
                        fndDepartments.TryGetValue(docu._Fld25263RRef, out val);
                        nf.DepartmentID = val;
                    }
                    else
                        throw new Exception();

                    nf.ConsignNumber = docu._Fld49984;

                    // Tech operations
                    if (fndTechOperation.ContainsKey(docu._Fld50224RRef))
                    {
                        Int64 val;
                        fndTechOperation.TryGetValue(docu._Fld50224RRef, out val);
                        nf.TechOperationID = val;
                    }
                    //else
                    //    throw new Exception();

                    nf.IDRRef = docu._IDRRef;
                    nf.Version = docu._Version;

                    if (docu._Posted == posted)
                        nf.Active = true;
                    else
                        nf.Active = false;
                    dataContext.DocTransferProductions.InsertOnSubmit(nf);
                    dataContext.SubmitChanges();

                    impFactTransferToProductionLines(docu._IDRRef);

                } // if (fndDocs.Contains(cheque._IDRRef) == true)

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;

            }

            dataContext.SubmitChanges();

        } // public void impFactTransferToProduction()

        private void impFactTransferToProductionLines(System.Data.Linq.Binary docIDRRef)
        {
            var doc = (from c in dataContext.DocTransferProductions
                          where c.IDRRef == docIDRRef
                          && c.TransferType == true
                          select c).FirstOrDefault();

            var docLns = from c in dataContextS1._Document741_VT25274s
                            where c._Document741_IDRRef == docIDRRef
                            select c;

            int i = 0;
            foreach (_Document741_VT25274 ln in docLns)
            {
                DocTransferProductionLine nl = new DocTransferProductionLine();
                nl.TransferType = true; //+
                nl.DocID = doc.ID;

                // Goods
                if (fndGoods.ContainsKey(ln._Fld25276RRef))
                {
                    Int64 val;
                    fndGoods.TryGetValue(ln._Fld25276RRef, out val);
                    nl.GoodID = val;
                }
                else
                    throw new Exception();

                nl.BaseQty = ln._Fld25282;

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

                nl.QtyPcs = ln._Fld45652;

                nl.Document741_IDRRef = ln._Document741_IDRRef;
                nl.KeyField = ln._KeyField;
                nl.LineNum = ln._LineNo25275;
                nl.Active = true;
                dataContext.DocTransferProductionLines.InsertOnSubmit(nl);

            } // foreach (_Document741_VT25274 ln in docLns)
        }

        public void impFactTransferToProductionDel()
        {
            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> loadHist = new HashSet<System.Data.Linq.Binary>();

            // Marked deleted
            byte[] posted = new byte[1];
            posted[0] = 1;
            // Marked deleted

            // From 1C 
            var transDocs = from t in dataContextS1._Document741s
                            where t._Posted == posted
                            select t;

            foreach (_Document741 doc in transDocs)
            {
                loadHist.Add(doc._IDRRef);
            }
            // -

            var fHists = from f in dataContext.DocTransferProductions
                         where f.TransferType == true
                         select f;

            int i = 0;
            foreach (DocTransferProduction rh in fHists)
            {

                if (loadHist.Contains(rh.IDRRef) == true)
                {
                    i++;
                    continue;
                }

                //rh.Active = false;
                impFactTransferToProductionLinesDel(rh.IDRRef);
                dataContext.DocTransferProductions.DeleteOnSubmit(rh);

                if (i % 50 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();
        }

        private void impFactTransferToProductionLinesDel(System.Data.Linq.Binary docIDRRef)
        {
            var chequeLns = from c in dataContext.DocTransferProductionLines
                            where c.Document741_IDRRef == docIDRRef
                            && c.TransferType == true
                            select c;

            dataContext.DocTransferProductionLines.DeleteAllOnSubmit(chequeLns);
        }

        // returns

        public void impFactTransferFromProduction()
        {

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndDocs = new HashSet<System.Data.Linq.Binary>();
            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                            System.Data.Linq.Binary>> fndDocVer = new HashSet<Tuple<System.Data.Linq.Binary,
                            System.Data.Linq.Binary>>();


            Dictionary<System.Data.Linq.Binary, Int64> fndWarehouses = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndDepartments = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndTechOperation = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<DateTime, int> fndDateKey = new Dictionary<DateTime, int>();

            // Fill documents
            var docs = from d in dataContext.DocTransferProductions
                       where d.TransferType == false
                       select d;
            foreach (DocTransferProduction doc in docs)
            {
                fndDocs.Add(doc.IDRRef);
            }
            foreach (DocTransferProduction doc in docs)
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

            // Tech operations
            var fOps = from g in dataContext.DimTechOperations
                       select g;
            foreach (DimTechOperation gd in fOps)
            {
                fndTechOperation.Add(gd.IDRRef, gd.ID);
            }
            // Tech operations

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


            var docus = from c in dataContextS1._Document562s
                        where c._Posted == posted
                        select c;

            int i = 0;
            foreach (_Document562 docu in docus)
            {

                if (fndDocVer.Contains(Tuple.Create(docu._IDRRef, docu._Version)) == true)
                {
                    i++;
                    continue;
                }

                if (fndDocs.Contains(docu._IDRRef) == true)
                {
                    // UPD
                    var nf = (from n in dataContext.DocTransferProductions
                              where n.IDRRef == docu._IDRRef
                              && n.TransferType == false
                              select n).FirstOrDefault();

                    nf.TransferType = false; // -

                    nf.NumberPrefix = docu._NumberPrefix;
                    nf.DocNumber = docu._Number;
                    nf.DocDate = docu._Date_Time.AddYears(-2000);

                    // DateKey
                    if (fndDateKey.ContainsKey(nf.DocDate.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(nf.DocDate.Value.Date, out val);
                        nf.DateKey = val;
                    }
                    else
                        throw new Exception();

                    nf.Posted = docu._Posted;

                    // Warehouses
                    if (fndWarehouses.ContainsKey(docu._Fld14899RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(docu._Fld14899RRef, out val);
                        nf.WarehouseID = val;
                    }
                    else
                        throw new Exception();

                    // Department
                    if (fndDepartments.ContainsKey(docu._Fld14900RRef))
                    {
                        Int64 val;
                        fndDepartments.TryGetValue(docu._Fld14900RRef, out val);
                        nf.DepartmentID = val;
                    }
                    else
                        throw new Exception();

                    nf.ConsignNumber = docu._Fld49989;

                    // Tech operations
                    if (fndTechOperation.ContainsKey(docu._Fld50223RRef))
                    {
                        Int64 val;
                        fndTechOperation.TryGetValue(docu._Fld50223RRef, out val);
                        nf.TechOperationID = val;
                    }
                    //else
                    //    throw new Exception();

                    nf.IDRRef = docu._IDRRef;
                    nf.Version = docu._Version;

                    if (docu._Posted == posted)
                        nf.Active = true;
                    else
                        nf.Active = false;

                    // update lines
                    var lines = from l in dataContext.DocTransferProductionLines
                                where l.Document741_IDRRef == docu._IDRRef
                                && l.TransferType == false
                                select l;
                    dataContext.DocTransferProductionLines.DeleteAllOnSubmit(lines);
                    dataContext.SubmitChanges();

                    impFactTransferFromProductionLines(docu._IDRRef);

                }
                else
                {
                    // NEW
                    DocTransferProduction nf = new DocTransferProduction();

                    nf.TransferType = false; // -

                    nf.NumberPrefix = docu._NumberPrefix;
                    nf.DocNumber = docu._Number;
                    nf.DocDate = docu._Date_Time.AddYears(-2000);

                    // DateKey
                    if (fndDateKey.ContainsKey(nf.DocDate.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(nf.DocDate.Value.Date, out val);
                        nf.DateKey = val;
                    }
                    else
                        throw new Exception();

                    nf.Posted = docu._Posted;

                    // Warehouses
                    if (fndWarehouses.ContainsKey(docu._Fld14899RRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(docu._Fld14899RRef, out val);
                        nf.WarehouseID = val;
                    }
                    else
                        throw new Exception();

                    // Department
                    if (fndDepartments.ContainsKey(docu._Fld14900RRef))
                    {
                        Int64 val;
                        fndDepartments.TryGetValue(docu._Fld14900RRef, out val);
                        nf.DepartmentID = val;
                    }
                    else
                        throw new Exception();

                    nf.ConsignNumber = docu._Fld49989;

                    // Tech operations
                    if (fndTechOperation.ContainsKey(docu._Fld50223RRef))
                    {
                        Int64 val;
                        fndTechOperation.TryGetValue(docu._Fld50223RRef, out val);
                        nf.TechOperationID = val;
                    }
                    //else
                    //    throw new Exception();

                    nf.IDRRef = docu._IDRRef;
                    nf.Version = docu._Version;

                    if (docu._Posted == posted)
                        nf.Active = true;
                    else
                        nf.Active = false;
                    dataContext.DocTransferProductions.InsertOnSubmit(nf);
                    dataContext.SubmitChanges();

                    impFactTransferFromProductionLines(docu._IDRRef);

                } // if (fndDocs.Contains(cheque._IDRRef) == true)

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;

            }

            dataContext.SubmitChanges();
        }

        public void impFactTransferFromProductionDel()
        {
            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> loadHist = new HashSet<System.Data.Linq.Binary>();

            // Marked deleted
            byte[] posted = new byte[1];
            posted[0] = 1;
            // Marked deleted

            // From 1C 
            var transDocs = from t in dataContextS1._Document562s
                            where t._Posted == posted
                            select t;

            foreach (_Document562 doc in transDocs)
            {
                loadHist.Add(doc._IDRRef);
            }
            // -

            var fHists = from f in dataContext.DocTransferProductions
                         where f.TransferType == false // -
                         select f;

            int i = 0;
            foreach (DocTransferProduction rh in fHists)
            {

                if (loadHist.Contains(rh.IDRRef) == true)
                {
                    i++;
                    continue;
                }

                //rh.Active = false;
                impFactTransferToProductionLinesDel(rh.IDRRef);
                dataContext.DocTransferProductions.DeleteOnSubmit(rh);

                if (i % 50 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();
        }

        private void impFactTransferFromProductionLines(System.Data.Linq.Binary docIDRRef)
        {
            var doc = (from c in dataContext.DocTransferProductions
                       where c.IDRRef == docIDRRef
                       && c.TransferType == false
                       select c).FirstOrDefault();

            var docLns = from c in dataContextS1._Document562_VT14907s
                         where c._Document562_IDRRef == docIDRRef
                         select c;

            int i = 0;
            foreach (_Document562_VT14907 ln in docLns)
            {
                DocTransferProductionLine nl = new DocTransferProductionLine();
                nl.TransferType = false;
                nl.DocID = doc.ID;

                // Goods
                if (fndGoods.ContainsKey(ln._Fld14909RRef))
                {
                    Int64 val;
                    fndGoods.TryGetValue(ln._Fld14909RRef, out val);
                    nl.GoodID = val;
                }
                else
                    throw new Exception();

                nl.BaseQty = ln._Fld14913 * (-1);

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

                nl.QtyPcs = ln._Fld45650 * (-1);

                nl.Document741_IDRRef = ln._Document562_IDRRef;
                nl.KeyField = ln._KeyField;
                nl.LineNum = ln._LineNo14908;
                nl.Active = true;
                dataContext.DocTransferProductionLines.InsertOnSubmit(nl);

            } // foreach (_Document562_VT14907 ln in docLns)
        }

        private void impFactTransferFromProductionLinesDel(System.Data.Linq.Binary docIDRRef)
        {
            var chequeLns = from c in dataContext.DocTransferProductionLines
                            where c.Document741_IDRRef == docIDRRef
                            && c.TransferType == false
                            select c;

            dataContext.DocTransferProductionLines.DeleteAllOnSubmit(chequeLns);
        }


    }
}
