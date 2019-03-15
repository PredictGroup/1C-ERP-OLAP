using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace ERPReportUtils
{
    public class FactManufactureImporter
    {
        DataClasses1DataContext dataContext;
        s1_DataClasses1DataContext dataContextS1;

        Int64 unitIDGramm = 18; // ID for unit Gramm

        public FactManufactureImporter()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();
        }

        // import movements from warehouses to production departments
        public void impFactMaterialsInProduction()
        {

            DateTime now = DateTime.Now;

            HashSet<Tuple<DateTime,
                System.Data.Linq.Binary, // TRef
                System.Data.Linq.Binary, // RRef
                decimal, // LineNo
                decimal, // Qty
                bool, // Active
                System.Data.Linq.Binary, // GoodRef
                Tuple<System.Data.Linq.Binary> // Department RRRef
                >> fndMatersFull = new HashSet<Tuple<DateTime,
                                    System.Data.Linq.Binary,
                                    System.Data.Linq.Binary,
                                    decimal,
                                    decimal,
                                    bool,
                                    System.Data.Linq.Binary,
                                    Tuple<System.Data.Linq.Binary>>>();

            HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>> fndMaters = new HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>>();

            Dictionary<System.Data.Linq.Binary, Int64> fndGoods = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndDepartments = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndSpecs = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<DateTime, int> fndDateKey = new Dictionary<DateTime, int>();

            Dictionary<Int64, Int64?> edIzmerId = new Dictionary<Int64, Int64?>();

            // Remains History
            var fRemHist = from f in dataContext.FactMaterialsInProductions
                           select f;
            foreach (FactMaterialsInProduction rh in fRemHist)
            {
                fndMaters.Add(Tuple.Create(rh.RecorderTRef, rh.RecorderRRef, rh.LineNum));
            }
            foreach (FactMaterialsInProduction rh in fRemHist)
            {
                fndMatersFull.Add(Tuple.Create(rh.Period.Value,
                                                rh.RecorderTRef,
                                                rh.RecorderRRef,
                                                rh.LineNum,
                                                rh.BaseQty.Value,
                                                rh.Active.Value,
                                                rh.GoodRef,
                                                rh.Department_RRRef));
            }
            // Remains History

            // ed izm
            var edIzms = from e in dataContext.DimGoods
                         select e;
            foreach (DimGoods ed in edIzms)
            {
                edIzmerId.Add(ed.ID, ed.BaseUnitID);

            }
            // ed izm


            // Goods
            var fGoods = from g in dataContext.DimGoods
                         select g;
            foreach (DimGoods gd in fGoods)
            {
                fndGoods.Add(gd.IDRRef, gd.ID);
            }
            // Goods

            // Departments
            var fDepartments = from g in dataContext.DimDepartments
                               select g;
            foreach (DimDepartments fd in fDepartments)
            {
                fndDepartments.Add(fd.IDRRef, fd.ID);
            }
            // Departments

            // Specifications
            var fSpecs = from g in dataContext.DimSpecifications
                         select g;
            foreach (DimSpecifications gd in fSpecs)
            {
                fndSpecs.Add(gd.IDRRef, gd.ID);
            }
            // Specifications

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

            // Department type 1 (internal departments)
            byte[] proizV1 = new byte[4];
            proizV1[0] = 0;
            proizV1[1] = 0;
            proizV1[2] = 1;
            proizV1[3] = 179;
            // Department type 1

            // Department type 2 (partners)
            byte[] proizV2 = new byte[4];
            proizV2[0] = 0;
            proizV2[1] = 0;
            proizV2[2] = 1;
            proizV2[3] = 22;
            // Department type 2

            var documents = from d in dataContextS1._AccumRg42790s
                            select d;

            int i = 0;
            foreach (_AccumRg42790 doc in documents)
            {
                if (fndMatersFull.Contains(Tuple.Create(doc._Period,
                            doc._RecorderTRef,
                            doc._RecorderRRef,
                            doc._LineNo,
                            doc._Fld42798, // Qty
                            true,
                            doc._Fld42792RRef,
                            doc._Fld42794_RRRef)) == true)
                { // not found or not active
                    i++;
                    continue;
                }

                if (fndMaters.Contains(Tuple.Create(doc._RecorderTRef, doc._RecorderRRef, doc._LineNo)) == true)
                {
                    // UPD
                    var fm = (from r in dataContext.FactMaterialsInProductions
                              where r.RecorderTRef == doc._RecorderTRef
                              && r.RecorderRRef == doc._RecorderRRef
                              && r.LineNum == doc._LineNo
                              select r).FirstOrDefault();

                    // Departments
                    if (doc._Fld42794_RTRef == proizV1)
                    {
                        if (fndDepartments.ContainsKey(doc._Fld42794_RRRef))
                        {
                            Int64 val;
                            fndDepartments.TryGetValue(doc._Fld42794_RRRef, out val);
                            fm.DepartmentID = val;
                        }
                        else
                            throw new Exception();
                    }
                    else
                        if (doc._Fld42794_RTRef == proizV2) // Partners
                    {
                        fm.DepartmentID = null;
                    }


                    // Goods
                    if (fndGoods.ContainsKey(doc._Fld42792RRef))
                    {
                        Int64 val;
                        fndGoods.TryGetValue(doc._Fld42792RRef, out val);
                        fm.GoodID = val;
                    }
                    else
                        throw new Exception();

                    fm.ConsignNumber = doc._Fld49992;

                    fm.Date = doc._Period.AddYears(-2000); // !!! before DateKey

                    // DateKey
                    if (fndDateKey.ContainsKey(fm.Date.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(fm.Date.Value.Date, out val);
                        fm.DateKey = val;
                    }
                    else
                        throw new Exception();

                    fm.BaseQty = doc._Fld42798;

                    // Units
                    var edIzmBId = (from r in edIzmerId
                                    where r.Key == fm.GoodID.Value
                                    select r).FirstOrDefault();

                    fm.BaseUnitID = edIzmBId.Value;

                    if (fm.BaseUnitID != unitIDGramm)
                    {
                        var unitConvs = (from u in dataContext.DimUnitConversion
                                         where u.FromUnitID == fm.BaseUnitID
                                         && u.ToUnitID == unitIDGramm
                                         select u).FirstOrDefault();
                        if (unitConvs == null)
                            fm.Qty = Math.Round(fm.BaseQty.Value, 3);
                        else
                            fm.Qty = Math.Round((unitConvs.ToQty.Value * fm.BaseQty.Value), 3);
                    }
                    else
                    {
                        fm.Qty = Math.Round(fm.BaseQty.Value, 3);
                    }
                    fm.UnitID = unitIDGramm;

                    fm.QtyPcs = doc._Fld49978;
                    fm.Period = doc._Period;

                    //int documentRef = BitConverter.ToInt32(doc._RecorderTRef.ToArray(), 0);

                    fm.RecorderTRef = doc._RecorderTRef;
                    fm.RecorderRRef = doc._RecorderRRef;
                    fm.LineNum = doc._LineNo;
                    fm.RecordKind = doc._RecordKind;
                    fm.GoodRef = doc._Fld42792RRef;
                    fm.Department_TYPE = doc._Fld42794_TYPE;
                    fm.Department_RTRef = doc._Fld42794_RTRef;
                    fm.Department_RRRef = doc._Fld42794_RRRef;

                    // Spec
                    if (fndSpecs.ContainsKey(doc._Fld42807RRef))
                    {
                        Int64 val;
                        fndSpecs.TryGetValue(doc._Fld42807RRef, out val);
                        fm.SpecID = val;
                    }
                    //else
                    //    throw new Exception();

                    if (fndDepartments.ContainsKey(doc._Fld42810RRef))
                    {
                        Int64 val;
                        fndDepartments.TryGetValue(doc._Fld42810RRef, out val);
                        fm.ToDepartmentID = val;
                    }
                    //else
                    //    throw new Exception();

                    if (doc._Active == mActive)
                        fm.Active = true;
                    else
                        fm.Active = false;
                }
                else
                {
                    // NEW 
                    FactMaterialsInProduction fm = new FactMaterialsInProduction();

                    // Departments
                    if (doc._Fld42794_RTRef == proizV1)
                    {
                        if (fndDepartments.ContainsKey(doc._Fld42794_RRRef))
                        {
                            Int64 val;
                            fndDepartments.TryGetValue(doc._Fld42794_RRRef, out val);
                            fm.DepartmentID = val;
                        }
                        else
                            throw new Exception();
                    }
                    else
                        if (doc._Fld42794_RTRef == proizV2) // Partners
                    {
                        fm.DepartmentID = null;
                    }


                    // Goods
                    if (fndGoods.ContainsKey(doc._Fld42792RRef))
                    {
                        Int64 val;
                        fndGoods.TryGetValue(doc._Fld42792RRef, out val);
                        fm.GoodID = val;
                    }
                    else
                        throw new Exception();

                    fm.ConsignNumber = doc._Fld49992;

                    fm.Date = doc._Period.AddYears(-2000); // !!! before DateKey

                    // DateKey
                    if (fndDateKey.ContainsKey(fm.Date.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(fm.Date.Value.Date, out val);
                        fm.DateKey = val;
                    }
                    else
                        throw new Exception();

                    fm.BaseQty = doc._Fld42798;

                    // Units
                    var edIzmBId = (from r in edIzmerId
                                    where r.Key == fm.GoodID.Value
                                    select r).FirstOrDefault();

                    fm.BaseUnitID = edIzmBId.Value;

                    if (fm.BaseUnitID != unitIDGramm)
                    {
                        var unitConvs = (from u in dataContext.DimUnitConversion
                                         where u.FromUnitID == fm.BaseUnitID
                                         && u.ToUnitID == unitIDGramm
                                         select u).FirstOrDefault();
                        if (unitConvs == null)
                            fm.Qty = Math.Round(fm.BaseQty.Value, 3);
                        else
                            fm.Qty = Math.Round((unitConvs.ToQty.Value * fm.BaseQty.Value), 3);
                    }
                    else
                    {
                        fm.Qty = Math.Round(fm.BaseQty.Value, 3);
                    }
                    fm.UnitID = unitIDGramm;

                    fm.QtyPcs = doc._Fld49978;
                    fm.Period = doc._Period;

                    //int documentRef = BitConverter.ToInt32(doc._RecorderTRef.ToArray(), 0);

                    fm.RecorderTRef = doc._RecorderTRef;
                    fm.RecorderRRef = doc._RecorderRRef;
                    fm.LineNum = doc._LineNo;
                    fm.RecordKind = doc._RecordKind;
                    fm.GoodRef = doc._Fld42792RRef;
                    fm.Department_TYPE = doc._Fld42794_TYPE;
                    fm.Department_RTRef = doc._Fld42794_RTRef;
                    fm.Department_RRRef = doc._Fld42794_RRRef;

                    // Spec
                    if (fndSpecs.ContainsKey(doc._Fld42807RRef))
                    {
                        Int64 val;
                        fndSpecs.TryGetValue(doc._Fld42807RRef, out val);
                        fm.SpecID = val;
                    }
                    //else
                    //    throw new Exception();

                    if (fndDepartments.ContainsKey(doc._Fld42810RRef))
                    {
                        Int64 val;
                        fndDepartments.TryGetValue(doc._Fld42810RRef, out val);
                        fm.ToDepartmentID = val;
                    }
                    //else
                    //    throw new Exception();

                    if (doc._Active == mActive)
                        fm.Active = true;
                    else
                        fm.Active = false;

                    dataContext.FactMaterialsInProductions.InsertOnSubmit(fm);

                } // if (fndMaters.Contains(Tuple.Create(doc._RecorderTRef, doc._RecorderRRef, doc._LineNo)) == true)

                if (i % 250 == 0)
                    dataContext.SubmitChanges();
                i++;
            } // foreach(_AccumRg42790 doc in documents)
            dataContext.SubmitChanges();

        } // public void impFactMaterialsInProduction()

        public void impFactMaterialsInProductionDel()
        {

            DateTime now = DateTime.Now;

            HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>> loadMatersHist = new HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>>();

            // From 1C 
            var matsInProds = from t in dataContextS1._AccumRg42790s
                              select t;
            foreach (_AccumRg42790 rh in matsInProds)
            {
                loadMatersHist.Add(Tuple.Create(rh._RecorderTRef, rh._RecorderRRef, rh._LineNo));
            }
            // From 1C 

            var fMatersHist = from f in dataContext.FactMaterialsInProductions
                           select f;

            int i = 0;
            foreach (FactMaterialsInProduction rh in fMatersHist)
            {

                if (loadMatersHist.Contains(Tuple.Create(rh.RecorderTRef, rh.RecorderRRef, rh.LineNum)) == true)
                {
                    i++;
                    continue;
                }

                //rh.Active = false;
                dataContext.FactMaterialsInProductions.DeleteOnSubmit(rh);

                if (i % 50 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        } // public void impFactMaterialsInProductionDel()

        // from Fact materials in production to production remains
        public void fromFactMaterialsToRemains()
        {
            //--------> DELETE ALL REMAINS 
            deleteAllRemains();

            DateTime now = DateTime.Now;
            DateTime onDate = DateTime.Today; // Remains on current date

            Dictionary<Int64, string> edIzmer = new Dictionary<Int64, string>();
            Dictionary<Int64, Int64?> edIzmerId = new Dictionary<Int64, Int64?>();

            // ed izm
            var edIzms = from e in dataContext.DimGoods
                         select e;
            foreach (DimGoods ed in edIzms)
            {
                edIzmerId.Add(ed.ID, ed.BaseUnitID);
                edIzmer.Add(ed.ID, ed.BaseUnit);
            }
            // ed izm

            deleteRemainsOnDate(onDate); // Fast delete from DB

            var dimDates = (from d in dataContext.DimDates
                            where d.Date == onDate
                            select d).FirstOrDefault();
            if (dimDates == null)
                throw new Exception();


            var remHistIn = from f in dataContext.FactMaterialsInProductions
                            where f.RecordKind == 0
                            && f.Active == true
                            group f by new
                            {
                                f.DepartmentID,
                                f.ConsignNumber,
                                f.GoodID,
                                f.Date
                            }
                            into g
                            select new
                            {
                                DepartmentID = g.Key.DepartmentID,
                                ConsignNumber = g.Key.ConsignNumber,
                                GoodID = g.Key.GoodID,
                                Date = g.Key.Date,
                                Qty = g.Sum(t => t.Qty),
                                QtyPcs = g.Sum(t => t.QtyPcs)
                            };
            var remHistOut = from f in dataContext.FactMaterialsInProductions
                             where f.RecordKind == 1
                             && f.Active == true
                             group f by new
                             {
                                 f.DepartmentID,
                                 f.ConsignNumber,
                                 f.GoodID,
                                 f.Date
                             }
                            into g
                             select new
                             {
                                 DepartmentID = g.Key.DepartmentID,
                                 ConsignNumber = g.Key.ConsignNumber,
                                 GoodID = g.Key.GoodID,
                                 Date = g.Key.Date,
                                 Qty = g.Sum(t => t.Qty) * (-1),
                                 QtyPcs = g.Sum(t => t.QtyPcs) * (-1)
                             };

            var remHistT = (from r in remHistIn
                            where r.Qty != 0
                            select r).Concat(from r in remHistOut
                                             where r.Qty != 0
                                             select r);
            var remHist = from r in remHistT
                          where r.Qty != 0
                          group r by new
                          {
                              r.DepartmentID,
                              r.ConsignNumber,
                              r.GoodID
                          }
                          into g
                          select new
                          {
                              DepartmentID = g.Key.DepartmentID,
                              ConsignNumber = g.Key.ConsignNumber,
                              GoodID = g.Key.GoodID,
                              Qty = g.Sum(t => t.Qty),
                              QtyPcs = g.Sum(t => t.QtyPcs)
                          };

            int i = 0;
            foreach (var rh in remHist)
            {
                FactProductionRemain rm = new FactProductionRemain();
                rm.DepartmentID = rh.DepartmentID;
                rm.ConsignNumber = rh.ConsignNumber;
                rm.GoodID = rh.GoodID;
                rm.RemainsDate = onDate;
                rm.DateKey = dimDates.DateKey;
                rm.Qty = rh.Qty;
                rm.QtyPcs = rh.QtyPcs;

                // Unit
                rm.UnitID = unitIDGramm;
                // Unit

                rm.Active = true;

                dataContext.FactProductionRemains.InsertOnSubmit(rm);

                if (i % 250 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

            deleteQtyZeroRemains();
        }

        private void deleteRemainsOnDate(DateTime onDate)
        {
            SqlConnection sqlConnection1 = new SqlConnection(global::ERPReportUtils.Properties.Settings.Default.ERPReportDBConnectionString);
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;

            cmd.CommandText = "DELETE FROM [APReport].[dbo].[FactProductionRemains] WHERE RemainsDate = '" + onDate.ToString() + "'";
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

            cmd.CommandText = "DELETE FROM [APReport].[dbo].[FactProductionRemains]";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();

            reader = cmd.ExecuteReader();
            // Data is accessible through the DataReader object here.

            sqlConnection1.Close();
        }

        private void deleteQtyZeroRemains()
        {
            SqlConnection sqlConnection1 = new SqlConnection(global::ERPReportUtils.Properties.Settings.Default.ERPReportDBConnectionString);
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;

            cmd.CommandText = "DELETE FROM [APReport].[dbo].[FactProductionRemains] WHERE [Qty] = 0";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();

            reader = cmd.ExecuteReader();
            // Data is accessible through the DataReader object here.

            sqlConnection1.Close();
        }

        // import Good Analytical Keys
        private void impDimGoodAnalyticalKeys()
        {
            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndKeys = new HashSet<System.Data.Linq.Binary>();
            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                            System.Data.Linq.Binary>> fndKeysVer = new HashSet<Tuple<System.Data.Linq.Binary,
                            System.Data.Linq.Binary>>();

            Dictionary<System.Data.Linq.Binary, Int64> fndGoods = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndWarehouses = new Dictionary<System.Data.Linq.Binary, Int64>();

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

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            // Good keys
            var fKeys = from g in dataContext.DimGoodAnalyticalKeys
                        select g;
            foreach (DimGoodAnalyticalKeys gd in fKeys)
            {
                fndKeys.Add(gd.IDRRef);
            }
            foreach (DimGoodAnalyticalKeys gd in fKeys)
            {
                fndKeysVer.Add(Tuple.Create(gd.IDRRef, gd.Version));
            }
            // Good keys

            var keys1c = from c in dataContextS1._Reference190
                         select c;

            int i = 0;
            foreach (_Reference190 key in keys1c)
            {

                if (fndKeysVer.Contains(Tuple.Create(key._IDRRef, key._Version)) == true)
                {
                    i++;
                    continue;
                }

                if (fndKeys.Contains(key._IDRRef) == true)
                {
                    var dg = (from g in dataContext.DimGoodAnalyticalKeys
                              where g.IDRRef == key._IDRRef
                              select g).FirstOrDefault();

                    // to Warehouses
                    if (fndWarehouses.ContainsKey(key._Fld5279_RRRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(key._Fld5279_RRRef, out val);
                        dg.WarehouseID = val;
                    }
                    //else
                    //    throw new Exception();

                    // Goods
                    if (fndGoods.ContainsKey(key._Fld5276RRef))
                    {
                        Int64 val;
                        fndGoods.TryGetValue(key._Fld5276RRef, out val);
                        dg.GoodID = val;
                    }
                    else
                        throw new Exception();

                    dg.Version = key._Version;

                    if (key._Marked == deleted)
                        dg.Active = true;
                    else
                        dg.Active = false;
                }
                else
                {
                    DimGoodAnalyticalKeys dg = new DimGoodAnalyticalKeys();

                    // to Warehouses
                    if (fndWarehouses.ContainsKey(key._Fld5279_RRRef))
                    {
                        Int64 val;
                        fndWarehouses.TryGetValue(key._Fld5279_RRRef, out val);
                        dg.WarehouseID = val;
                    }
                    //else
                    //    throw new Exception();

                    // Goods
                    if (fndGoods.ContainsKey(key._Fld5276RRef))
                    {
                        Int64 val;
                        fndGoods.TryGetValue(key._Fld5276RRef, out val);
                        dg.GoodID = val;
                    }
                    else
                        throw new Exception();

                    dg.IDRRef = key._IDRRef;
                    dg.Version = key._Version;

                    if (key._Marked == deleted)
                        dg.Active = true;
                    else
                        dg.Active = false;

                    dataContext.DimGoodAnalyticalKeys.InsertOnSubmit(dg);
                }

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        } // private void impDimGoodAnalyticalKeys()

        // import materials production process
        public void impFactManufacture()
        {
            impDimGoodAnalyticalKeys();

            DateTime now = DateTime.Now;

            HashSet<Tuple<DateTime,
                System.Data.Linq.Binary, // TRef
                System.Data.Linq.Binary, // RRef
                decimal, // LineNo
                decimal, // Qty
                System.Data.Linq.Binary, // SpecificationRef
                System.Data.Linq.Binary,
                Tuple<System.Data.Linq.Binary> // Department RRRef, АналитикаУчетаНоменклатуры
                >> fndProdFull = new HashSet<Tuple<DateTime,
                                    System.Data.Linq.Binary,
                                    System.Data.Linq.Binary,
                                    decimal,
                                    decimal,
                                    System.Data.Linq.Binary,
                                    System.Data.Linq.Binary,
                                    Tuple<System.Data.Linq.Binary>>>();

            HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>> fndProd = new HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>>();

            Dictionary<System.Data.Linq.Binary, Int64> fndGoods = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndDepartments = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndWarehouses = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<System.Data.Linq.Binary, Int64> fndSpecs = new Dictionary<System.Data.Linq.Binary, Int64>();
            Dictionary<DateTime, int> fndDateKey = new Dictionary<DateTime, int>();

            Dictionary<Int64, Int64?> edIzmerId = new Dictionary<Int64, Int64?>();

            // FactProduction History
            var fFactProduction = from f in dataContext.FactProductions
                                  select f;
            foreach (FactProduction rh in fFactProduction)
            {
                fndProd.Add(Tuple.Create(rh.RecorderTRef, rh.RecorderRRef, rh.LineNum));
            }
            foreach (FactProduction rh in fFactProduction)
            {
                fndProdFull.Add(Tuple.Create(rh.Period.Value,
                                                rh.RecorderTRef,
                                                rh.RecorderRRef,
                                                rh.LineNum,
                                                rh.BaseQty.Value,
                                                rh.SpecificationRef,
                                                rh.DepartmentRef,
                                                rh.GoodAnalyticalKeyRef));
            }
            // FactProduction History

            // ed izm
            var edIzms = from e in dataContext.DimGoods
                         select e;
            foreach (DimGoods ed in edIzms)
            {
                edIzmerId.Add(ed.ID, ed.BaseUnitID);

            }
            // ed izm


            // Goods
            var fGoods = from g in dataContext.DimGoods
                         select g;
            foreach (DimGoods gd in fGoods)
            {
                fndGoods.Add(gd.IDRRef, gd.ID);
            }
            // Goods

            // Departments
            var fDepartments = from g in dataContext.DimDepartments
                               select g;
            foreach (DimDepartments fd in fDepartments)
            {
                fndDepartments.Add(fd.IDRRef, fd.ID);
            }
            // Departments

            // Warehouses
            var fWarehouses = from g in dataContext.DimWarehouses
                              select g;
            foreach (DimWarehouses wh in fWarehouses)
            {
                fndWarehouses.Add(wh.IDRRef, wh.ID);
            }
            // Warehouses

            // Specifications
            var fSpecs = from g in dataContext.DimSpecifications
                         select g;
            foreach (DimSpecifications gd in fSpecs)
            {
                fndSpecs.Add(gd.IDRRef, gd.ID);
            }
            // Units

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

            var productionProc = from t in dataContextS1._AccumRg41567s
                                 select t;

            int i = 0;
            foreach (_AccumRg41567 doc in productionProc)
            {
                if (fndProdFull.Contains(Tuple.Create(doc._Period,
                                            doc._RecorderTRef,
                                            doc._RecorderRRef,
                                            doc._LineNo,
                                            doc._Fld41578, // Qty
                                            doc._Fld41574RRef, // Spec
                                            doc._Fld41575RRef, // Dept
                                            doc._Fld41569RRef)) == true) // GoodAnaliticalKey
                { // not found or not active
                    i++;
                    continue;
                }

                if (fndProd.Contains(Tuple.Create(doc._RecorderTRef, doc._RecorderRRef, doc._LineNo)) == true)
                {
                    // UPD
                    var fp = (from r in dataContext.FactProductions
                              where r.RecorderTRef == doc._RecorderTRef
                              && r.RecorderRRef == doc._RecorderRRef
                              && r.LineNum == doc._LineNo
                              select r).FirstOrDefault();

                    var goodAnalKey = (from k in dataContext.DimGoodAnalyticalKeys
                                       where k.IDRRef == doc._Fld41569RRef
                                       select k).FirstOrDefault();

                    fp.ToWarehouseID = goodAnalKey.WarehouseID;

                    // from Department
                    if (fndDepartments.ContainsKey(doc._Fld41575RRef))
                    {
                        Int64 val;
                        fndDepartments.TryGetValue(doc._Fld41575RRef, out val);
                        fp.FromDepartmentID = val;
                    }
                    //else
                    //    throw new Exception();

                    fp.GoodID = goodAnalKey.GoodID;

                    fp.ConsignNumber = doc._Fld49991;

                    fp.Date = doc._Period.AddYears(-2000);

                    // DateKey
                    if (fndDateKey.ContainsKey(fp.Date.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(fp.Date.Value.Date, out val);
                        fp.DateKey = val;
                    }
                    else
                        throw new Exception();

                    fp.BaseQty = doc._Fld41578;

                    // Units
                    var edIzmBId = (from r in edIzmerId
                                    where r.Key == fp.GoodID.Value
                                    select r).FirstOrDefault();

                    fp.BaseUnitID = edIzmBId.Value;

                    if (fp.BaseUnitID != unitIDGramm)
                    {
                        var unitConvs = (from u in dataContext.DimUnitConversion
                                         where u.FromUnitID == fp.BaseUnitID
                                         && u.ToUnitID == unitIDGramm
                                         select u).FirstOrDefault();
                        if (unitConvs == null)
                            fp.Qty = Math.Round(fp.BaseQty.Value, 3);
                        else
                            fp.Qty = Math.Round((unitConvs.ToQty.Value * fp.BaseQty.Value), 3);
                    }
                    else
                    {
                        fp.Qty = Math.Round(fp.BaseQty.Value, 3);
                    }
                    fp.UnitID = unitIDGramm;

                    fp.QtyPcs = doc._Fld49977;

                    // Spec
                    if (fndSpecs.ContainsKey(doc._Fld41574RRef))
                    {
                        Int64 val;
                        fndSpecs.TryGetValue(doc._Fld41574RRef, out val);
                        fp.SpecificationID = val;
                    }
                    //else
                    //    throw new Exception();

                    fp.Period = doc._Period;
                    fp.SpecificationRef = doc._Fld41574RRef;
                    fp.DepartmentRef = doc._Fld41575RRef;
                    fp.GoodAnalyticalKeyRef = doc._Fld41569RRef;

                    if (doc._Active == mActive)
                        fp.Active = true;
                    else
                        fp.Active = false;

                }
                else
                {
                    // NEW

                    FactProduction fp = new FactProduction();

                    var goodAnalKey = (from k in dataContext.DimGoodAnalyticalKeys
                                       where k.IDRRef == doc._Fld41569RRef
                                       select k).FirstOrDefault();

                    fp.ToWarehouseID = goodAnalKey.WarehouseID;

                    // from Department
                    if (fndDepartments.ContainsKey(doc._Fld41575RRef))
                    {
                        Int64 val;
                        fndDepartments.TryGetValue(doc._Fld41575RRef, out val);
                        fp.FromDepartmentID = val;
                    }
                    //else
                    //    throw new Exception();

                    fp.GoodID = goodAnalKey.GoodID;

                    fp.ConsignNumber = doc._Fld49991;

                    fp.Date = doc._Period.AddYears(-2000);

                    // DateKey
                    if (fndDateKey.ContainsKey(fp.Date.Value.Date))
                    {
                        int val;
                        fndDateKey.TryGetValue(fp.Date.Value.Date, out val);
                        fp.DateKey = val;
                    }
                    else
                        throw new Exception();

                    fp.BaseQty = doc._Fld41578;

                    // Units
                    var edIzmBId = (from r in edIzmerId
                                    where r.Key == fp.GoodID.Value
                                    select r).FirstOrDefault();

                    fp.BaseUnitID = edIzmBId.Value;

                    if (fp.BaseUnitID != unitIDGramm)
                    {
                        var unitConvs = (from u in dataContext.DimUnitConversion
                                         where u.FromUnitID == fp.BaseUnitID
                                         && u.ToUnitID == unitIDGramm
                                         select u).FirstOrDefault();
                        if (unitConvs == null)
                            fp.Qty = Math.Round(fp.BaseQty.Value, 3);
                        else
                            fp.Qty = Math.Round((unitConvs.ToQty.Value * fp.BaseQty.Value), 3);
                    }
                    else
                    {
                        fp.Qty = Math.Round(fp.BaseQty.Value, 3);
                    }
                    fp.UnitID = unitIDGramm;

                    fp.QtyPcs = doc._Fld49977;

                    // Spec
                    if (fndSpecs.ContainsKey(doc._Fld41574RRef))
                    {
                        Int64 val;
                        fndSpecs.TryGetValue(doc._Fld41574RRef, out val);
                        fp.SpecificationID = val;
                    }
                    //else
                    //    throw new Exception();

                    fp.Period = doc._Period;
                    fp.RecorderTRef = doc._RecorderTRef;
                    fp.RecorderRRef = doc._RecorderRRef;
                    fp.LineNum = doc._LineNo;
                    fp.SpecificationRef = doc._Fld41574RRef;
                    fp.DepartmentRef = doc._Fld41575RRef;
                    fp.GoodAnalyticalKeyRef = doc._Fld41569RRef;

                    if (doc._Active == mActive)
                        fp.Active = true;
                    else
                        fp.Active = false;

                    dataContext.FactProductions.InsertOnSubmit(fp);
                }

                if (i % 250 == 0)
                    dataContext.SubmitChanges();
                i++;

            } // foreach (_AccumRg41567 pP in productionProc)
            dataContext.SubmitChanges();

        } // public void impFactManufacture()

        public void impFactManufactureDel()
        {

            DateTime now = DateTime.Now;

            HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>> loadManufHist = new HashSet<Tuple<System.Data.Linq.Binary, System.Data.Linq.Binary, decimal>>();

            // From 1C 
            var manufs = from t in dataContextS1._AccumRg41567s
                                  select t;

            foreach (_AccumRg41567 rh in manufs)
            {
                loadManufHist.Add(Tuple.Create(rh._RecorderTRef, rh._RecorderRRef, rh._LineNo));
            }
            // From 1C 

            var fManufHist = from f in dataContext.FactProductions
                           select f;

            int i = 0;
            foreach (FactProduction rh in fManufHist)
            {

                if (loadManufHist.Contains(Tuple.Create(rh.RecorderTRef, rh.RecorderRRef, rh.LineNum)) == true)
                {
                    i++;
                    continue;
                }

                //rh.Active = false;
                dataContext.FactProductions.DeleteOnSubmit(rh);

                if (i % 50 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        }

    } // public class FactManufactureImporter
}
