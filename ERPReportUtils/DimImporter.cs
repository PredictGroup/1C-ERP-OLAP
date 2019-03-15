using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Xml;
using System.Globalization;

namespace ERPReportUtils
{
    public class DimImporter
    {
        DataClasses1DataContext dataContext;
        s1_DataClasses1DataContext dataContextS1;

        public DimImporter()
        {

        }

        // Groups import
        public void impDimGroups()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndGroups = new HashSet<System.Data.Linq.Binary>();
            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                            System.Data.Linq.Binary>> fndGroupsVer = new HashSet<Tuple<System.Data.Linq.Binary,
                            System.Data.Linq.Binary>>();

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            // Groups
            var fGroups = from g in dataContext.DimGroups
                          select g;
            foreach (DimGroups gd in fGroups)
            {
                fndGroups.Add(gd.IDRRef);
            }
            foreach (DimGroups gd in fGroups)
            {
                fndGroupsVer.Add(Tuple.Create(gd.IDRRef, gd.Version));
            }
            // Groups

            byte[] bGroups = new byte[1];
            bGroups[0] = 0;

            var groups1c = from c in dataContextS1._Reference231s
                           where c._Folder == bGroups
                           select c;

            int i = 0;
            foreach (_Reference231 grp in groups1c)
            {

                if (fndGroupsVer.Contains(Tuple.Create(grp._IDRRef, grp._Version)) == true)
                {
                    i++;
                    continue;
                }

                if (fndGroups.Contains(grp._IDRRef) == true)
                {
                    var dg = (from g in dataContext.DimGroups
                              where g.IDRRef == grp._IDRRef
                              select g).FirstOrDefault();

                    dg.Name = grp._Description;
                    dg.Code = grp._Code;
                    dg.Version = grp._Version;

                    if (grp._Marked == deleted)
                        dg.Active = true;
                    else
                        dg.Active = false;
                }
                else
                {
                    DimGroups dg = new DimGroups();
                    dg.Name = grp._Description;
                    dg.Code = grp._Code;
                    dg.IDRRef = grp._IDRRef;
                    dg.Version = grp._Version;

                    if (grp._Marked == deleted)
                        dg.Active = true;
                    else
                        dg.Active = false;

                    dataContext.DimGroups.InsertOnSubmit(dg);
                }

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        }

        // Unit import
        public void impDimUnits()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndUnits = new HashSet<System.Data.Linq.Binary>();
            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                System.Data.Linq.Binary>> fndUnitsVer = new HashSet<Tuple<System.Data.Linq.Binary,
                System.Data.Linq.Binary>>();

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            // Units
            var fUnits = from g in dataContext.DimUnits
                         select g;
            foreach (DimUnits gd in fUnits)
            {
                fndUnits.Add(gd.IDRRef);
            }
            foreach (DimUnits gd in fUnits)
            {
                fndUnitsVer.Add(Tuple.Create(gd.IDRRef, gd.Version));
            }
            // Units

            var edIzms = from r in dataContextS1._Reference492
                         select r;

            int i = 0;
            foreach (_Reference492 ed in edIzms)
            {
                if (fndUnitsVer.Contains(Tuple.Create(ed._IDRRef, ed._Version)) == true)
                {
                    i++;
                    continue;
                }

                if (fndUnits.Contains(ed._IDRRef) == true)
                {
                    var dU = (from g in dataContext.DimUnits
                              where g.IDRRef == ed._IDRRef
                              select g).FirstOrDefault();

                    dU.Code = ed._Code;
                    dU.TypeName = ed._Description;
                    dU.Version = ed._Version;

                    if (ed._Marked == deleted)
                        dU.Active = false;
                    else
                        dU.Active = true;
                }
                else
                {
                    DimUnits dU = new DimUnits();
                    dU.Code = ed._Code;
                    dU.TypeName = ed._Description;

                    dU.IDRRef = ed._IDRRef;
                    dU.Version = ed._Version;

                    if (ed._Marked == deleted)
                        dU.Active = false;
                    else
                        dU.Active = true;
                    dataContext.DimUnits.InsertOnSubmit(dU);
                }

                dataContext.SubmitChanges();
                i++;
            }

        }
        // Unit import

        // Price Types import
        public void impDimPriceTypes()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndPriceTypes = new HashSet<System.Data.Linq.Binary>();

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            // Price Types
            var fPriceTypes = from g in dataContext.DimPriceTypes
                              select g;
            foreach (DimPriceTypes gd in fPriceTypes)
            {
                fndPriceTypes.Add(gd.IDRRef);
            }
            // Price Types

            var priceTypes1c = from c in dataContextS1._Reference102
                               select c;

            int i = 0;
            foreach (_Reference102 pt in priceTypes1c)
            {

                if (fndPriceTypes.Contains(pt._IDRRef) == true)
                {
                    i++;
                    continue;
                }

                DimPriceTypes nPt = new DimPriceTypes();
                nPt.TypeName = pt._Description;


                nPt.IDRRef = pt._IDRRef;
                nPt.Version = pt._Version;

                if (pt._Marked == deleted)
                    nPt.Active = true;
                else
                    nPt.Active = false;
                dataContext.DimPriceTypes.InsertOnSubmit(nPt);

                dataContext.SubmitChanges();
                i++;
            }

        }
        // Price Types import

        // Currency
        public void impDimCurrencies()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndCurrencies = new HashSet<System.Data.Linq.Binary>();

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            // find Currency
            var fCurrencies = from g in dataContext.DimCurrencies
                              select g;
            foreach (DimCurrencies gd in fCurrencies)
            {
                fndCurrencies.Add(gd.IDRRef);
            }
            // find Currency

            var curr1c = from c in dataContextS1._Reference51
                         select c;

            int i = 0;
            foreach (_Reference51 cy in curr1c)
            {

                if (fndCurrencies.Contains(cy._IDRRef) == true)
                {
                    i++;
                    continue;
                }

                DimCurrencies nCy = new DimCurrencies();
                nCy.Code = cy._Code;
                nCy.Name = cy._Description;
                nCy.FullName = cy._Fld2540;

                nCy.IDRRef = cy._IDRRef;
                nCy.Version = cy._Version;

                if (cy._Marked == deleted)
                    nCy.Active = true;
                else
                    nCy.Active = false;
                dataContext.DimCurrencies.InsertOnSubmit(nCy);

                dataContext.SubmitChanges();
                i++;
            }

        }
        // Currency

        // Goods import
        public void impDimGoods()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef
                                System.Data.Linq.Binary>> fndGoodsVer = new HashSet<Tuple<System.Data.Linq.Binary,
                                                                                            System.Data.Linq.Binary>>();
            HashSet<System.Data.Linq.Binary> fndGoods = new HashSet<System.Data.Linq.Binary>();

            Dictionary<System.Data.Linq.Binary, Int64> fndGroups = new Dictionary<System.Data.Linq.Binary, Int64>();

            Dictionary<System.Data.Linq.Binary, string> edIzmer = new Dictionary<System.Data.Linq.Binary, string>();
            Dictionary<System.Data.Linq.Binary, Int64> edIzmerId = new Dictionary<System.Data.Linq.Binary, Int64>();

            Dictionary<System.Data.Linq.Binary, decimal> nominalWghts = new Dictionary<System.Data.Linq.Binary, decimal>();
            Dictionary<System.Data.Linq.Binary, decimal> deviationWghts = new Dictionary<System.Data.Linq.Binary, decimal>();

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            // Goods
            var fGoods = from g in dataContext.DimGoods
                         select new { IDRRef = g.IDRRef, Version = g.Version };
            foreach (var gd in fGoods)
            {
                fndGoods.Add(gd.IDRRef);
            }
            foreach (var gd in fGoods)
            {
                fndGoodsVer.Add(Tuple.Create(gd.IDRRef,
                                                gd.Version));
            }
            // Goods

            // ed izm
            var edIzms = from r in dataContext.DimUnits
                         select r;
            foreach (DimUnits ed in edIzms)
            {
                edIzmer.Add(ed.IDRRef, ed.TypeName);
                edIzmerId.Add(ed.IDRRef, ed.ID);
            }
            // ed izm

            // Groups
            var groups = from g in dataContext.DimGroups
                         select g;
            foreach (DimGroups gr in groups)
            {
                fndGroups.Add(gr.IDRRef, gr.ID);
            }
            // Groups

            // NominalWeights & Deviation
            string input = "0x942200155D320A0011E5AD6C708631A3"; // Номинальная масса (Справочник "Номенклатура" (Общие))
            byte[] nWeigtByte = HexToBytes(input, 2, input.Length);

            var nominls = from n in dataContextS1._Reference231_VT6192s
                          where n._Fld6194RRef == nWeigtByte
                          select n;
            foreach (_Reference231_VT6192 nm in nominls)
            {
                nominalWghts.Add(nm._Reference231_IDRRef, nm._Fld6195_N);
            }

            input = "0x942200155D320A0011E5AD6C7A5E6C76"; // Допустимое отклонение от номинальной массы (Справочник "Номенклатура" (Общие))
            byte[] nDevByte = HexToBytes(input, 2, input.Length);
            var deviats = from n in dataContextS1._Reference231_VT6192s
                          where n._Fld6194RRef == nDevByte
                          select n;
            foreach (_Reference231_VT6192 dv in deviats)
            {
                deviationWghts.Add(dv._Reference231_IDRRef, dv._Fld6195_N);
            }
            // NominalWeights & Deviation

            byte[] bGoods = new byte[1];
            bGoods[0] = 1;

            var goods1c = from c in dataContextS1._Reference231s
                          where c._Folder == bGoods
                          select c;

            int i = 0;
            foreach (_Reference231 good in goods1c)
            {

                if (fndGoodsVer.Contains(Tuple.Create(good._IDRRef, good._Version)) == true)
                { // found
                    i++;
                    continue;
                }

                if (fndGoods.Contains(good._IDRRef) == true)
                {
                    // UPD

                    var dg = (from g in dataContext.DimGoods
                              where g.IDRRef == good._IDRRef
                              select g).FirstOrDefault();

                    dg.Code = good._Code;
                    dg.Name = good._Description;
                    dg.Artikul = good._Fld6116;

                    // BaseUnit
                    var edIzmB = (from r in edIzmer
                                  where r.Key == good._Fld6129RRef
                                  select r).FirstOrDefault();
                    if (edIzmB.Value != null)
                        dg.BaseUnit = edIzmB.Value;

                    var edIzmIdB = (from r in edIzmerId
                                    where r.Key == good._Fld6129RRef
                                    select r).FirstOrDefault();
                    if (edIzmIdB.Value != 0)
                        dg.BaseUnitID = edIzmIdB.Value;
                    else
                        throw new Exception();
                    // BaseUnit

                    // Weight for one unit in pcs
                    var edIzm = (from r in edIzmer
                                 where r.Key == good._Fld6118RRef
                                 select r).FirstOrDefault();

                    if (edIzm.Value != null)
                    {
                        if (edIzm.Value == "г")
                            dg.WeightGramm1Pcs = good._Fld6122;
                        else
                            if (edIzm.Value == "кг")
                            dg.WeightGramm1Pcs = good._Fld6122 * 1000;
                    }
                    else
                    {
                        dg.WeightGramm1Pcs = good._Fld6122;
                    }
                    // Weight for one unit in pcs

                    // NominalWeights & Deviation
                    var nomWght = (from w in nominalWghts
                                   where w.Key == good._IDRRef
                                   select w).FirstOrDefault();
                    if (nomWght.Key != null)
                        dg.WeightGrammNominal = nomWght.Value;

                    var devWght = (from w in deviationWghts
                                   where w.Key == good._IDRRef
                                   select w).FirstOrDefault();
                    if (devWght.Key != null)
                        dg.WeightGrammDeviation = devWght.Value;
                    // NominalWeights & Deviation

                    var group = (from g in fndGroups
                                 where g.Key == good._ParentIDRRef
                                 select g).FirstOrDefault();
                    if (group.Key != null)
                        dg.GroupID = group.Value;


                    dg.IDRRef = good._IDRRef;
                    dg.Version = good._Version;

                    if (good._Marked == deleted)
                        dg.Active = true;
                    else
                        dg.Active = false;
                }
                else
                {
                    // NEW

                    DimGoods dg = new DimGoods();
                    dg.Code = good._Code;
                    dg.Name = good._Description;
                    dg.Artikul = good._Fld6116;

                    // BaseUnit
                    var edIzmB = (from r in edIzmer
                                  where r.Key == good._Fld6129RRef
                                  select r).FirstOrDefault();
                    if (edIzmB.Value != null)
                        dg.BaseUnit = edIzmB.Value;

                    var edIzmIdB = (from r in edIzmerId
                                    where r.Key == good._Fld6129RRef
                                    select r).FirstOrDefault();
                    if (edIzmIdB.Value != 0)
                        dg.BaseUnitID = edIzmIdB.Value;
                    else
                        throw new Exception();
                    // BaseUnit

                    // Weight for one unit in pcs
                    var edIzm = (from r in edIzmer
                                 where r.Key == good._Fld6118RRef
                                 select r).FirstOrDefault();

                    if (edIzm.Value != null)
                    {
                        if (edIzm.Value == "г")
                            dg.WeightGramm1Pcs = good._Fld6122;
                        else
                            if (edIzm.Value == "кг")
                            dg.WeightGramm1Pcs = good._Fld6122 * 1000;
                    }
                    else
                    {
                        dg.WeightGramm1Pcs = good._Fld6122;
                    }
                    // Weight for one unit in pcs

                    // NominalWeights & Deviation
                    var nomWght = (from w in nominalWghts
                                   where w.Key == good._IDRRef
                                   select w).FirstOrDefault();
                    if (nomWght.Key != null)
                        dg.WeightGrammNominal = nomWght.Value;

                    var devWght = (from w in deviationWghts
                                   where w.Key == good._IDRRef
                                   select w).FirstOrDefault();
                    if (devWght.Key != null)
                        dg.WeightGrammDeviation = devWght.Value;
                    // NominalWeights & Deviation

                    var group = (from g in fndGroups
                                 where g.Key == good._ParentIDRRef
                                 select g).FirstOrDefault();
                    if (group.Key != null)
                        dg.GroupID = group.Value;

                    dg.IDRRef = good._IDRRef;
                    dg.Version = good._Version;

                    if (good._Marked == deleted)
                        dg.Active = true;
                    else
                        dg.Active = false;
                    dataContext.DimGoods.InsertOnSubmit(dg);
                }

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

            dimGoodsCorrections();
            dimGoodsNominalWeightCorr();
        }

        // DimGoods corrections
        private void dimGoodsCorrections()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            var goods = from g in dataContext.DimGoods
                        select g;

            int i = 0;
            foreach (DimGoods good in goods)
            {
                if (good.Artikul.Length > 0)
                {
                    if (good.Artikul.Substring(0, 1) == "_")
                    {
                        good.Artikul = good.Artikul.Substring(1, good.Artikul.Length - 1);

                        if (i % 100 == 0)
                            dataContext.SubmitChanges();
                        i++;
                    }
                }
            }
            dataContext.SubmitChanges();
        }

        private void dimGoodsNominalWeightCorr()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;


            HashSet<DimGoods> fndGoods = new HashSet<DimGoods>();

            Dictionary<System.Data.Linq.Binary, decimal> nominalWghts_N = new Dictionary<System.Data.Linq.Binary, decimal>();
            Dictionary<System.Data.Linq.Binary, decimal> deviationWghts_N = new Dictionary<System.Data.Linq.Binary, decimal>();

            Dictionary<System.Data.Linq.Binary, string> nominalWghts_S = new Dictionary<System.Data.Linq.Binary, string>();
            Dictionary<System.Data.Linq.Binary, string> deviationWghts_S = new Dictionary<System.Data.Linq.Binary, string>();


            // Goods
            var fGoods = from g in dataContext.DimGoods
                         where (g.BaseUnitID == 16 || g.BaseUnitID == 18)
                               && (g.WeightGrammNominal == null
                               || g.WeightGrammNominal <= 0)
                               //&& g.Artikul == "122925"
                         select g;
            foreach (var gd in fGoods)
            {
                fndGoods.Add(gd);
            }

            // Goods

            // NominalWeights & Deviation
            string input = "0x942200155D320A0011E5AD6C708631A3"; // Номинальная масса (Справочник "Номенклатура" (Общие))
            byte[] nWeigtByte = HexToBytes(input, 2, input.Length);

            var nominls = from n in dataContextS1._Reference231_VT6192s
                          where n._Fld6194RRef == nWeigtByte
                          select n;
            foreach (_Reference231_VT6192 nm in nominls)
            {
                nominalWghts_N.Add(nm._Reference231_IDRRef, nm._Fld6195_N);
                nominalWghts_S.Add(nm._Reference231_IDRRef, nm._Fld6195_S);
            }

            input = "0x942200155D320A0011E5AD6C7A5E6C76"; // Допустимое отклонение от номинальной массы (Справочник "Номенклатура" (Общие))
            byte[] nDevByte = HexToBytes(input, 2, input.Length);
            var deviats = from n in dataContextS1._Reference231_VT6192s
                          where n._Fld6194RRef == nDevByte
                          select n;
            foreach (_Reference231_VT6192 dv in deviats)
            {
                deviationWghts_N.Add(dv._Reference231_IDRRef, dv._Fld6195_N);
                deviationWghts_S.Add(dv._Reference231_IDRRef, dv._Fld6195_S);
            }
            // NominalWeights & Deviation

            byte[] bGoods = new byte[1];
            bGoods[0] = 1;


            int i = 0;
            foreach (DimGoods good in fndGoods)
            {

                // NominalWeights & Deviation
                var nomWght = (from w in nominalWghts_N
                               where w.Key == good.IDRRef
                               select w).FirstOrDefault();
                if (nomWght.Key != null)
                    good.WeightGrammNominal = nomWght.Value;

                if (good.WeightGrammNominal == 0 || good.WeightGrammNominal == null)
                {
                    var nomWght_S = (from w in nominalWghts_S
                                     where w.Key == good.IDRRef
                                     select w).FirstOrDefault();
                    if (nomWght_S.Value != null)
                    {
                        Decimal tmp = 0;
                        Decimal.TryParse(nomWght_S.Value.Replace(".", ","), out tmp);
                        good.WeightGrammNominal = tmp;
                    }
                }

                var devWght = (from w in deviationWghts_N
                               where w.Key == good.IDRRef
                               select w).FirstOrDefault();
                if (devWght.Key != null)
                    good.WeightGrammDeviation = devWght.Value;
                if (good.WeightGrammDeviation == 0 || good.WeightGrammDeviation == null)
                {
                    var devWght_S = (from w in deviationWghts_S
                                     where w.Key == good.IDRRef
                                     select w).FirstOrDefault();
                    if (devWght_S.Value != null)
                    {
                        Decimal tmp = 0;
                        Decimal.TryParse(devWght_S.Value.Replace(".", ","), out tmp);
                        good.WeightGrammDeviation = tmp;
                    }
                }
                // NominalWeights & Deviation


                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();
        }

        // From string to byte
        public byte[] HexToBytes(string hexEncodedBytes, int start, int end)
        {
            int length = end - start;
            const string tagName = "hex";
            string fakeXmlDocument = String.Format("<{1}>{0}</{1}>",
                                   hexEncodedBytes.Substring(start, length),
                                   tagName);
            var stream = new MemoryStream(Encoding.ASCII.GetBytes(fakeXmlDocument));
            XmlReader reader = XmlReader.Create(stream, new XmlReaderSettings());
            int hexLength = length / 2;
            byte[] result = new byte[hexLength];
            reader.ReadStartElement(tagName);
            reader.ReadContentAsBinHex(result, 0, hexLength);
            return result;
        }

        // Departments import
        public void impDimDepartments()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndDps = new HashSet<System.Data.Linq.Binary>();
            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef
                            System.Data.Linq.Binary>> fndDpsVer = new HashSet<Tuple<System.Data.Linq.Binary,
                            System.Data.Linq.Binary>>();

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            // Departments
            var depts = from g in dataContext.DimDepartments
                        select g;
            foreach (DimDepartments dp in depts)
            {
                fndDps.Add(dp.IDRRef);
            }
            // Departments

            var departs1c = from c in dataContextS1._Reference435
                            select c;

            int i = 0;
            foreach (_Reference435 dept in departs1c)
            {

                if (fndDpsVer.Contains(Tuple.Create(dept._IDRRef, dept._Version)) == true)
                {
                    i++;
                    continue;
                }

                if (fndDps.Contains(dept._IDRRef) == true)
                {
                    var dg = (from d in dataContext.DimDepartments
                              where d.IDRRef == dept._IDRRef
                              select d).FirstOrDefault();

                    dg.Code = dept._Code;
                    dg.Name = dept._Description;
                    dg.IDRRef = dept._IDRRef;
                    dg.Version = dept._Version;

                    if (dept._Marked == deleted)
                        dg.Active = true;
                    else
                        dg.Active = false;
                }
                else
                {
                    DimDepartments dg = new DimDepartments();
                    dg.Code = dept._Code;
                    dg.Name = dept._Description;
                    dg.IDRRef = dept._IDRRef;
                    dg.Version = dept._Version;

                    if (dept._Marked == deleted)
                        dg.Active = true;
                    else
                        dg.Active = false;

                    dataContext.DimDepartments.InsertOnSubmit(dg);
                }

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        }

        // Warehouses import
        public void impDimWarehouses()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndWhs = new HashSet<System.Data.Linq.Binary>();

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            // Warehouses
            var whs = from g in dataContext.DimWarehouses
                      select g;
            foreach (DimWarehouses wh in whs)
            {
                fndWhs.Add(wh.IDRRef);
            }
            // Warehouses

            var wh1c = from c in dataContextS1._Reference396s
                       select c;

            int i = 0;
            foreach (_Reference396 wh in wh1c)
            {
                if (fndWhs.Contains(wh._IDRRef) == true)
                {
                    i++;
                    continue;
                }

                DimWarehouses dg = new DimWarehouses();
                dg.Name = wh._Description;
                dg.IDRRef = wh._IDRRef;
                dg.Version = wh._Version;

                if (wh._Marked == deleted)
                    dg.Active = true;
                else
                    dg.Active = false;

                dataContext.DimWarehouses.InsertOnSubmit(dg);

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        }

        // POS registers import
        public void impDimPOSRegisters()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndPOSs = new HashSet<System.Data.Linq.Binary>();
            Dictionary<System.Data.Linq.Binary, Int64> fndWarehouses = new Dictionary<System.Data.Linq.Binary, Int64>();

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            // POS
            var poss = from g in dataContext.DimPOSRegisters
                       select g;
            foreach (DimPOSRegisters rg in poss)
            {
                fndPOSs.Add(rg.IDRRef);
            }
            // POS

            // Warehouses
            var fWarehouses = from g in dataContext.DimWarehouses
                              select g;
            foreach (DimWarehouses wh in fWarehouses)
            {
                fndWarehouses.Add(wh.IDRRef, wh.ID);
            }
            // Warehouses

            var pss = from c in dataContextS1._Reference180
                      select c;

            int i = 0;
            foreach (_Reference180 ps in pss)
            {
                if (fndPOSs.Contains(ps._IDRRef) == true)
                {
                    i++;
                    continue;
                }

                DimPOSRegisters pr = new DimPOSRegisters();
                pr.Name = ps._Description;

                // Warehouses
                if (fndWarehouses.ContainsKey(ps._Fld5190RRef))
                {
                    Int64 val;
                    fndWarehouses.TryGetValue(ps._Fld5190RRef, out val);
                    pr.WarehouseID = val;
                }
                else
                    throw new Exception();

                pr.IDRRef = ps._IDRRef;
                pr.Version = ps._Version;

                if (ps._Marked == deleted)
                    pr.Active = true;
                else
                    pr.Active = false;

                dataContext.DimPOSRegisters.InsertOnSubmit(pr);

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;
            }
            dataContext.SubmitChanges();

        }

        // Users importer
        public void impDimPeople()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndPeople = new HashSet<System.Data.Linq.Binary>();

            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                                System.Data.Linq.Binary>> fndPeopleVer = new HashSet<Tuple<System.Data.Linq.Binary,
                                System.Data.Linq.Binary>>();

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            // People fill
            var people = from p in dataContext.DimPeople
                         select p;
            foreach (DimPeople male in people)
            {
                fndPeople.Add(male.IDRRef);
            }
            foreach (DimPeople male in people)
            {
                fndPeopleVer.Add(Tuple.Create(male.IDRRef,
                                                male.Version));
            }
            // People fill

            var userList = from r in dataContextS1._Reference313
                           select r;

            int i = 0;
            foreach (_Reference313 usr in userList)
            {

                if (fndPeopleVer.Contains(Tuple.Create(usr._IDRRef, usr._Version)) == true)
                { // found
                    i++;
                    continue;
                }

                if (fndPeople.Contains(usr._IDRRef) == true)
                {
                    // UPD
                    var dp = (from p in dataContext.DimPeople
                              where p.IDRRef == usr._IDRRef
                              select p).FirstOrDefault();

                    dp.Name = usr._Description;
                    dp.IDRRef = usr._IDRRef;
                    dp.Version = usr._Version;

                    if (usr._Marked == deleted)
                        dp.Active = true;
                    else
                        dp.Active = false;

                }
                else
                {
                    // NEW
                    DimPeople dp = new DimPeople();

                    dp.Name = usr._Description;
                    dp.IDRRef = usr._IDRRef;
                    dp.Version = usr._Version;

                    if (usr._Marked == deleted)
                        dp.Active = true;
                    else
                        dp.Active = false;
                    dataContext.DimPeople.InsertOnSubmit(dp);

                }

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;
            } // foreach (_Reference313 usr in userList)

            dataContext.SubmitChanges();

        }

        // Spec import
        public void impDimSpec()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndSpec = new HashSet<System.Data.Linq.Binary>();
            Dictionary<System.Data.Linq.Binary, decimal> fndStatus = new Dictionary<System.Data.Linq.Binary, decimal>();
            Dictionary<System.Data.Linq.Binary, Int64> fndPeople = new Dictionary<System.Data.Linq.Binary, Int64>();

            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                                System.Data.Linq.Binary>> fndSpecVer = new HashSet<Tuple<System.Data.Linq.Binary,
                                System.Data.Linq.Binary>>();

            // Status
            var fStatus = from g in dataContextS1._Enum1448
                          select g;
            foreach (_Enum1448 gd in fStatus)
            {
                fndStatus.Add(gd._IDRRef, gd._EnumOrder);
            }
            // Status

            // Specifications fill
            var specs = from p in dataContext.DimSpecifications
                        select p;
            foreach (DimSpecifications sp in specs)
            {
                fndSpec.Add(sp.IDRRef);
            }
            foreach (DimSpecifications sp in specs)
            {
                fndSpecVer.Add(Tuple.Create(sp.IDRRef,
                                                sp.Version));
            }
            // Specifications fill

            // People
            var fPeople = from p in dataContext.DimPeople
                          select p;
            foreach (DimPeople dp in fPeople)
            {
                fndPeople.Add(dp.IDRRef, dp.ID);
            }
            // People

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            byte[] bGroups = new byte[1];
            bGroups[0] = 0;

            var specList = from r in dataContextS1._Reference371
                           where r._Folder != bGroups
                           select r;

            int i = 0;
            foreach (_Reference371 spec in specList)
            {

                if (fndSpecVer.Contains(Tuple.Create(spec._IDRRef, spec._Version)) == true)
                { // found
                    i++;
                    continue;
                }

                if (fndSpec.Contains(spec._IDRRef) == true)
                {
                    // UPD
                    var dp = (from p in dataContext.DimSpecifications
                              where p.IDRRef == spec._IDRRef
                              select p).FirstOrDefault();

                    dp.Code = spec._Code;
                    dp.Name = spec._Description;

                    // Status
                    if (spec._Fld9641RRef != null)
                    {
                        if (fndStatus.ContainsKey(spec._Fld9641RRef))
                        {
                            decimal val;
                            fndStatus.TryGetValue(spec._Fld9641RRef, out val);
                            if (val == 0)
                                dp.Status = "В разработке";
                            else
                                if (val == 1)
                                dp.Status = "Действует";
                            else
                                if (val == 2)
                                dp.Status = "Закрыта";
                        }
                        else
                            throw new Exception();
                    }

                    dp.FromDate = spec._Fld9642;
                    dp.ToDate = spec._Fld9643;

                    // People
                    if (fndPeople.ContainsKey(spec._Fld9645RRef))
                    {
                        Int64 val;
                        fndPeople.TryGetValue(spec._Fld9645RRef, out val);
                        dp.UserID = val;
                    }
                    else
                        throw new Exception();

                    dp.Description = spec._Fld9646;

                    dp.IDRRef = spec._IDRRef;
                    dp.Version = spec._Version;

                    if (spec._Marked == deleted)
                        dp.Active = true;
                    else
                        dp.Active = false;

                }
                else
                {
                    // NEW
                    DimSpecifications dp = new DimSpecifications();

                    dp.Code = spec._Code;
                    dp.Name = spec._Description;

                    // Status
                    if (spec._Fld9641RRef != null)
                    {
                        if (fndStatus.ContainsKey(spec._Fld9641RRef))
                        {
                            decimal val;
                            fndStatus.TryGetValue(spec._Fld9641RRef, out val);
                            if (val == 0)
                                dp.Status = "В разработке";
                            else
                                if (val == 1)
                                dp.Status = "Действует";
                            else
                                if (val == 2)
                                dp.Status = "Закрыта";
                        }
                        else
                            throw new Exception();
                    }

                    dp.FromDate = spec._Fld9642;
                    dp.ToDate = spec._Fld9643;

                    // People
                    if (fndPeople.ContainsKey(spec._Fld9645RRef))
                    {
                        Int64 val;
                        fndPeople.TryGetValue(spec._Fld9645RRef, out val);
                        dp.UserID = val;
                    }
                    else
                        throw new Exception();

                    dp.Description = spec._Fld9646;

                    dp.IDRRef = spec._IDRRef;
                    dp.Version = spec._Version;

                    if (spec._Marked == deleted)
                        dp.Active = true;
                    else
                        dp.Active = false;

                    dataContext.DimSpecifications.InsertOnSubmit(dp);

                }

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;
            } // foreach (_Reference371 spec in specList)

            dataContext.SubmitChanges();

        } // public void impDimSpec()

        public void impDimSpecLines()
        {

        } // public void impDimSpecLines()

        // Technological operations
        public void impDimTechOperations()
        {

            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndOperation = new HashSet<System.Data.Linq.Binary>();

            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                                System.Data.Linq.Binary>> fndOperationVer = new HashSet<Tuple<System.Data.Linq.Binary,
                                System.Data.Linq.Binary>>();

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            // People fill
            var ops = from p in dataContext.DimTechOperations
                      select p;
            foreach (DimTechOperation op in ops)
            {
                fndOperation.Add(op.IDRRef);
            }
            foreach (DimTechOperation op in ops)
            {
                fndOperationVer.Add(Tuple.Create(op.IDRRef,
                                                op.Version));
            }
            // People fill

            var operList = from r in dataContextS1._Reference50219s
                           select r;

            int i = 0;
            foreach (_Reference50219 op in operList)
            {

                if (fndOperationVer.Contains(Tuple.Create(op._IDRRef, op._Version)) == true)
                { // found
                    i++;
                    continue;
                }

                if (fndOperation.Contains(op._IDRRef) == true)
                {
                    // UPD
                    var dp = (from p in dataContext.DimTechOperations
                              where p.IDRRef == op._IDRRef
                              select p).FirstOrDefault();

                    dp.Code = op._Code;
                    dp.Name = op._Description;
                    dp.IDRRef = op._IDRRef;
                    dp.Version = op._Version;

                    if (op._Marked == deleted)
                        dp.Active = true;
                    else
                        dp.Active = false;

                }
                else
                {
                    // NEW
                    DimTechOperation dp = new DimTechOperation();

                    dp.Code = op._Code;
                    dp.Name = op._Description;
                    dp.IDRRef = op._IDRRef;
                    dp.Version = op._Version;

                    if (op._Marked == deleted)
                        dp.Active = true;
                    else
                        dp.Active = false;

                    dataContext.DimTechOperations.InsertOnSubmit(dp);

                }

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;

            } // foreach (_Reference313 usr in userList)

            dataContext.SubmitChanges();

        }

        // Partners
        public void impDimPartners()
        {

            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            HashSet<System.Data.Linq.Binary> fndPartners = new HashSet<System.Data.Linq.Binary>();

            HashSet<Tuple<System.Data.Linq.Binary, // IDRRef, Version
                                System.Data.Linq.Binary>> fndPartnersVer = new HashSet<Tuple<System.Data.Linq.Binary,
                                System.Data.Linq.Binary>>();

            // Marked deleted
            byte[] deleted = new byte[1];
            deleted[0] = 1;
            // Marked deleted

            // People fill
            var ops = from p in dataContext.DimPartners
                      select p;
            foreach (DimPartner op in ops)
            {
                fndPartners.Add(op.IDRRef);
            }
            foreach (DimPartner op in ops)
            {
                fndPartnersVer.Add(Tuple.Create(op.IDRRef,
                                                op.Version));
            }
            // People fill

            var partnerList = from r in dataContextS1._Reference278s
                              select r;

            int i = 0;
            foreach (_Reference278 pn in partnerList)
            {

                if (fndPartnersVer.Contains(Tuple.Create(pn._IDRRef, pn._Version)) == true)
                { // found
                    i++;
                    continue;
                }

                if (fndPartners.Contains(pn._IDRRef) == true)
                {
                    // UPD
                    var dp = (from p in dataContext.DimPartners
                              where p.IDRRef == pn._IDRRef
                              select p).FirstOrDefault();

                    dp.Code = pn._Code;
                    dp.Name = pn._Description;
                    dp.IDRRef = pn._IDRRef;
                    dp.Version = pn._Version;

                    if (pn._Marked == deleted)
                        dp.Active = true;
                    else
                        dp.Active = false;

                }
                else
                {
                    // NEW
                    DimPartner dp = new DimPartner();

                    dp.Code = pn._Code;
                    dp.Name = pn._Description;
                    dp.FullName = pn._Fld7247;
                    dp.IDRRef = pn._IDRRef;
                    dp.Version = pn._Version;

                    if (pn._Marked == deleted)
                        dp.Active = true;
                    else
                        dp.Active = false;

                    dataContext.DimPartners.InsertOnSubmit(dp);

                }

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;

            } // foreach (_Reference278 pn in partnerList)

            dataContext.SubmitChanges();

        }

        // Currency Rates
        public void impFactCurrencyRates()
        {

            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            DateTime now = DateTime.Now;

            Dictionary<System.Data.Linq.Binary, Int64> fndCurrency = new Dictionary<System.Data.Linq.Binary, Int64>();

            HashSet<Tuple<DateTime, // datetime, currency id
                                System.Data.Linq.Binary>> fndCurrencyRate = new HashSet<Tuple<DateTime,
                                System.Data.Linq.Binary>>();


            // Curr fill
            var currencies = from p in dataContext.DimCurrencies
                             select p;
            foreach (DimCurrencies curr in currencies)
            {
                fndCurrency.Add(curr.IDRRef, curr.ID);
            }

            var rates = from p in dataContext.FactCurrencyRates
                        select p;
            foreach (FactCurrencyRate rate in rates)
            {
                fndCurrencyRate.Add(Tuple.Create(rate.Period.Value,
                                                rate.CurrencyRRef));
            }


            var ldRates = from r in dataContextS1._InfoRg37054s
                          select r;

            int i = 0;
            foreach (_InfoRg37054 rt in ldRates)
            {

                if (fndCurrencyRate.Contains(Tuple.Create(rt._Period, rt._Fld37055RRef)) == true)
                { // found
                    i++;
                    continue;
                }

                // NEW
                FactCurrencyRate fr = new FactCurrencyRate();

                // Curr
                if (fndCurrency.ContainsKey(rt._Fld37055RRef))
                {
                    Int64 val;
                    fndCurrency.TryGetValue(rt._Fld37055RRef, out val);
                    fr.CurrencyID = val;
                }
                else
                    throw new Exception();

                fr.Period = rt._Period;
                fr.Date = rt._Period.AddYears(-2000); // !!! before DateKey
                fr.Rate = rt._Fld37056;
                fr.Fold = rt._Fld37057;
                fr.CurrencyRRef = rt._Fld37055RRef;

                dataContext.FactCurrencyRates.InsertOnSubmit(fr);

                if (i % 100 == 0)
                    dataContext.SubmitChanges();
                i++;

            } // foreach (_InfoRg37054 rt in ldRates)

            dataContext.SubmitChanges();

        }



    } // public class DimImporter
}