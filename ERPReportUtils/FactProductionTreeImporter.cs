using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace ERPReportUtils
{
    public class FactProductionTreeImporter
    {
        DataClasses1DataContext dataContext;
        s1_DataClasses1DataContext dataContextS1;

        Int64 unitIDGramm = 18; // ID for unit Gramm

        int iter = 0;

        public FactProductionTreeImporter()
        {
            dataContext = new DataClasses1DataContext();
            dataContextS1 = new s1_DataClasses1DataContext();

            deleteAllProductionTree();
        }

        public void impFillProductionTree()
        {
            var docProds = from p in dataContext.DocProductions
                           orderby p.DocDate ascending
                           select p;
            // Идем по всем выпускам продукции

            int i = 0;
            foreach (DocProduction dProd in docProds)
            {
                var SpecID = (from s in dataContext.DocProductionLines
                              where s.DocID == dProd.ID
                              select s.SpecificationID).FirstOrDefault(); // Спека

                // выбираем все списания затрат на выпуск (документы)
                var docProdCost = (from p in dataContext.DocProductionCosts
                                   where p.SpecificationID == SpecID
                                   select p).FirstOrDefault();
                if (docProdCost == null)
                    continue;

                var dCostOutGr = (from l in dataContext.DocProductionCostOut
                                  where l.DocID == docProdCost.ID
                                  group l by l.DocID into g
                                  select new
                                  {
                                      BaseQty = g.Sum(_ => _.BaseQty),
                                      Qty = g.Sum(_ => _.Qty),
                                      QtyPcs = g.Sum(_ => _.QtyPcs)
                                  }).FirstOrDefault(); // Итоги по документу 

                var dCostInGr = (from l in dataContext.DocProductionCostIn
                                 where l.DocID == docProdCost.ID
                                 group l by l.DocID into g
                                 select new
                                 {
                                     BaseQty = g.Sum(_ => _.BaseQty),
                                     Qty = g.Sum(_ => _.Qty),
                                     QtyPcs = g.Sum(_ => _.QtyPcs)
                                 }).FirstOrDefault(); // Итоги по документу 

                var dCostMatGr = (from l in dataContext.DocProductionCostMaterials
                                  where l.DocID == docProdCost.ID
                                  group l by l.DocID into g
                                  select new
                                  {
                                      BaseQty = g.Sum(_ => _.BaseQty),
                                      Qty = g.Sum(_ => _.Qty),
                                      QtyPcs = g.Sum(_ => _.QtyPcs)
                                  }).FirstOrDefault(); // Итоги по документу 

                var dCostOutLns = from l in dataContext.DocProductionCostOut
                                  where l.DocID == docProdCost.ID
                                  select l;
                foreach (DocProductionCostOut dCostOutLn in dCostOutLns)
                { // по выходным изделиям распределяем затраты

                    var dCostMatLns = from l in dataContext.DocProductionCostMaterials
                                      where l.DocID == docProdCost.ID
                                      select l; // по материалам сырья 
                    foreach (DocProductionCostMaterials dCostMatLn in dCostMatLns)
                    {
                        FactProductionTree tree = new FactProductionTree();
                        tree.ProductionDocID = dProd.ID;
                        tree.CostDocID = docProdCost.ID;
                        tree.CostOutLineID = dCostOutLn.ID;
                        tree.CostMatLineID = dCostMatLn.ID;

                        tree.WarehouseID = dProd.WarehouseID;
                        tree.DepartmentID = dCostMatLn.DepartmentID;
                        tree.Date = dProd.DocDate;
                        tree.DateKey = dProd.DateKey;
                        tree.ConsignNumber = dProd.ConsignNumber;

                        decimal dMatValue = Math.Round(dCostMatLn.Qty.Value / dCostMatGr.Qty.Value, 15);
                        decimal dOutValue = Math.Round(dCostOutLn.Qty.Value / dCostOutGr.Qty.Value, 15);

                        // Сырье
                        tree.RawGoodID = dCostMatLn.GoodID;
                        tree.RawBaseQty = Math.Round(dCostMatLn.BaseQty.Value * dOutValue, 3);
                        tree.RawBaseUnitID = dCostMatLn.BaseUnitID;
                        if (dCostMatLn.BaseUnitID != unitIDGramm)
                        {
                            var unitConvs = (from u in dataContext.DimUnitConversion
                                             where u.FromUnitID == dCostMatLn.BaseUnitID
                                             && u.ToUnitID == unitIDGramm
                                             select u).FirstOrDefault();
                            if (unitConvs == null)
                                tree.RawQty = Math.Round(dCostMatLn.BaseQty.Value * dOutValue, 3);
                            else
                                tree.RawQty = Math.Round((unitConvs.ToQty.Value * dCostMatLn.BaseQty.Value) * dOutValue, 3);
                        }
                        else
                        {
                            tree.RawQty = Math.Round(dCostMatLn.BaseQty.Value * dOutValue, 3);
                        }
                        tree.RemainQty = tree.RawQty.Value;

                        tree.RawUnitID = unitIDGramm;
                        tree.RawQtyPcs = dCostMatLn.QtyPcs;
                        tree.RemainQtyPcs = tree.RawQtyPcs.Value;

                        // Выпущенный полуфабрикат
                        tree.PrepackGoodID = dCostOutLn.GoodID;
                        tree.PrepackBaseQty = dCostOutLn.BaseQty * dMatValue;
                        tree.PrepackBaseUnitID = dCostOutLn.BaseUnitID;
                        if (dCostOutLn.BaseUnitID != unitIDGramm)
                        {
                            var unitConvs = (from u in dataContext.DimUnitConversion
                                             where u.FromUnitID == dCostOutLn.BaseUnitID
                                             && u.ToUnitID == unitIDGramm
                                             select u).FirstOrDefault();
                            if (unitConvs == null)
                                tree.PrepackQty = Math.Round(dCostOutLn.BaseQty.Value * dMatValue, 3);
                            else
                                tree.PrepackQty = Math.Round((unitConvs.ToQty.Value * dCostOutLn.BaseQty.Value) * dMatValue, 3);
                        }
                        else
                        {
                            tree.PrepackQty = Math.Round(dCostOutLn.BaseQty.Value * dMatValue, 3);
                        }
                        tree.PrepackUnitID = unitIDGramm;
                        tree.PrepackQtyPcs = dCostOutLn.QtyPcs;

                        if (tree.RawQty.Value != 0)
                            tree.PercentProduction = Math.Round((tree.PrepackQty.Value / tree.RawQty.Value) * 100, 3);
                        else
                            tree.PercentProduction = 0;

                        tree.Active = true;

                        dataContext.FactProductionTree.InsertOnSubmit(tree);
                        if (i % 250 == 0)
                            dataContext.SubmitChanges();
                        i++;
                    }

                } // foreach(DocProductionCostOut dCostOutLn in dCostOutLns)

            } // foreach(DocProduction docProd in docProds)

            dataContext.SubmitChanges();

        } // public void impFillProductionTree()

        private void deleteAllProductionTree()
        {
            SqlConnection sqlConnection1 = new SqlConnection(global::ERPReportUtils.Properties.Settings.Default.ERPReportDBConnectionString);
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;

            cmd.CommandText = "DELETE FROM [APReport].[dbo].[FactProductionTree]";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlConnection1;

            sqlConnection1.Open();

            reader = cmd.ExecuteReader();
            // Data is accessible through the DataReader object here.

            sqlConnection1.Close();
        }


        // Обработчик по партиям
        public void impFillProductionTreeByConsignments()
        {
            var docProds = from p in dataContext.DocProductions
                           where p.ConsignNumber != "" // только с заполненным занчением партий
                           orderby p.ConsignNumber ascending,p.DocDate ascending
                           select p;
            // Идем по всем выпускам продукции

            int i = 0;
            foreach (DocProduction dProd in docProds)
            {
                var SpecID = (from s in dataContext.DocProductionLines
                              where s.DocID == dProd.ID
                              select s.SpecificationID).FirstOrDefault(); // Спека

                // выбираем все списания затрат на выпуск (документы)
                var docProdCost = (from p in dataContext.DocProductionCosts
                                   where p.SpecificationID == SpecID
                                   select p).FirstOrDefault();
                if (docProdCost == null)
                    continue;

                var dCostOutGr = (from l in dataContext.DocProductionCostOut
                                  where l.DocID == docProdCost.ID
                                  group l by l.DocID into g
                                  select new
                                  {
                                      BaseQty = g.Sum(_ => _.BaseQty),
                                      Qty = g.Sum(_ => _.Qty),
                                      QtyPcs = g.Sum(_ => _.QtyPcs)
                                  }).FirstOrDefault(); // Итоги по документу 

                var dCostInGr = (from l in dataContext.DocProductionCostIn
                                 where l.DocID == docProdCost.ID
                                 group l by l.DocID into g
                                 select new
                                 {
                                     BaseQty = g.Sum(_ => _.BaseQty),
                                     Qty = g.Sum(_ => _.Qty),
                                     QtyPcs = g.Sum(_ => _.QtyPcs)
                                 }).FirstOrDefault(); // Итоги по документу 

                var dCostMatGr = (from l in dataContext.DocProductionCostMaterials
                                  where l.DocID == docProdCost.ID
                                  group l by l.DocID into g
                                  select new
                                  {
                                      BaseQty = g.Sum(_ => _.BaseQty),
                                      Qty = g.Sum(_ => _.Qty),
                                      QtyPcs = g.Sum(_ => _.QtyPcs)
                                  }).FirstOrDefault(); // Итоги по документу 

                var dCostOutLns = from l in dataContext.DocProductionCostOut
                                  where l.DocID == docProdCost.ID
                                  select l;
                foreach (DocProductionCostOut dCostOutLn in dCostOutLns)
                { // по выходным изделиям распределяем затраты

                    var dCostMatLns = from l in dataContext.DocProductionCostMaterials
                                      where l.DocID == docProdCost.ID
                                      select l; // по материалам сырья 
                    foreach (DocProductionCostMaterials dCostMatLn in dCostMatLns)
                    {
                        FactProductionTree tree = new FactProductionTree();
                        tree.ProductionDocID = dProd.ID;
                        tree.CostDocID = docProdCost.ID;
                        tree.CostOutLineID = dCostOutLn.ID;
                        tree.CostMatLineID = dCostMatLn.ID;

                        tree.WarehouseID = dProd.WarehouseID;
                        tree.DepartmentID = dCostMatLn.DepartmentID;
                        tree.Date = dProd.DocDate;
                        tree.DateKey = dProd.DateKey;
                        tree.ConsignNumber = dProd.ConsignNumber;

                        decimal dMatValue = Math.Round(dCostMatLn.Qty.Value / dCostMatGr.Qty.Value, 15);
                        decimal dOutValue = Math.Round(dCostOutLn.Qty.Value / dCostOutGr.Qty.Value, 15);

                        // Сырье
                        tree.RawGoodID = dCostMatLn.GoodID;
                        tree.RawBaseQty = Math.Round(dCostMatLn.BaseQty.Value * dOutValue, 3);
                        tree.RawBaseUnitID = dCostMatLn.BaseUnitID;
                        if (dCostMatLn.BaseUnitID != unitIDGramm)
                        {
                            var unitConvs = (from u in dataContext.DimUnitConversion
                                             where u.FromUnitID == dCostMatLn.BaseUnitID
                                             && u.ToUnitID == unitIDGramm
                                             select u).FirstOrDefault();
                            if (unitConvs == null)
                                tree.RawQty = Math.Round(dCostMatLn.BaseQty.Value * dOutValue, 3);
                            else
                                tree.RawQty = Math.Round((unitConvs.ToQty.Value * dCostMatLn.BaseQty.Value) * dOutValue, 3);
                        }
                        else
                        {
                            tree.RawQty = Math.Round(dCostMatLn.BaseQty.Value * dOutValue, 3);
                        }
                        tree.RemainQty = tree.RawQty.Value;

                        tree.RawUnitID = unitIDGramm;
                        tree.RawQtyPcs = dCostMatLn.QtyPcs;
                        tree.RemainQtyPcs = tree.RawQtyPcs.Value;

                        // Выпущенный полуфабрикат
                        tree.PrepackGoodID = dCostOutLn.GoodID;
                        tree.PrepackBaseQty = dCostOutLn.BaseQty * dMatValue;
                        tree.PrepackBaseUnitID = dCostOutLn.BaseUnitID;
                        if (dCostOutLn.BaseUnitID != unitIDGramm)
                        {
                            var unitConvs = (from u in dataContext.DimUnitConversion
                                             where u.FromUnitID == dCostOutLn.BaseUnitID
                                             && u.ToUnitID == unitIDGramm
                                             select u).FirstOrDefault();
                            if (unitConvs == null)
                                tree.PrepackQty = Math.Round(dCostOutLn.BaseQty.Value * dMatValue, 3);
                            else
                                tree.PrepackQty = Math.Round((unitConvs.ToQty.Value * dCostOutLn.BaseQty.Value) * dMatValue, 3);
                        }
                        else
                        {
                            tree.PrepackQty = Math.Round(dCostOutLn.BaseQty.Value * dMatValue, 3);
                        }
                        tree.PrepackUnitID = unitIDGramm;
                        tree.PrepackQtyPcs = dCostOutLn.QtyPcs;

                        if (tree.RawQty.Value != 0)
                            tree.PercentProduction = Math.Round((tree.PrepackQty.Value / tree.RawQty.Value) * 100, 3);
                        else
                            tree.PercentProduction = 0;

                        tree.Active = true;

                        dataContext.FactProductionTree.InsertOnSubmit(tree);
                        if (i % 250 == 0)
                            dataContext.SubmitChanges();
                        i++;
                    }

                } // foreach(DocProductionCostOut dCostOutLn in dCostOutLns)

            } // foreach(DocProduction docProd in docProds)

            dataContext.SubmitChanges();

        } // public void impFillProductionTree()


    } // public class FactProductionTreeImporter
}
