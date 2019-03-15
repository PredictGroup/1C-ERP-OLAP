using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace ERPReportUtils
{
    /// <summary>
    /// Логика взаимодействия для MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
        }

        private void btnCloseForm_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        private void btnImportDimGroups_Click(object sender, RoutedEventArgs e)
        {
            // Import dimensions
            DimImporter dimClass = new DimImporter();

            // Groups
            dimClass.impDimGroups();
        }

        private void btnImportDimGoods_Click(object sender, RoutedEventArgs e)
        {
            // Import dimensions
            DimImporter dimClass = new DimImporter();

            // Goods
            dimClass.impDimGoods();
        }

        private void btnImportDimDepartments_Click(object sender, RoutedEventArgs e)
        {
            // Import dimensions
            DimImporter dimClass = new DimImporter();

            // Departments
            dimClass.impDimDepartments();
        }

        private void btnImportDimWarehouses_Click(object sender, RoutedEventArgs e)
        {
            // Import dimensions
            DimImporter dimClass = new DimImporter();

            // Warehouses
            dimClass.impDimWarehouses();
        }

        private void btnImportAll_Click(object sender, RoutedEventArgs e)
        {
            // Import dimensions
            DimImporter dimClass = new DimImporter();

            // Groups
            dimClass.impDimGroups();
            // Goods
            dimClass.impDimUnits();
            dimClass.impDimCurrencies();
            dimClass.impFactCurrencyRates();
            dimClass.impDimPriceTypes();
            dimClass.impDimGoods();
            // Departments
            dimClass.impDimDepartments();
            // Warehouses
            dimClass.impDimWarehouses();
            // POS
            dimClass.impDimPOSRegisters();
            // People
            dimClass.impDimPeople();
            // Spec
            dimClass.impDimSpec();
            // Ops
            dimClass.impDimTechOperations();
            // Pn
            dimClass.impDimPartners();
        }

        private void btnImportFactRemains_Click(object sender, RoutedEventArgs e)
        {
            // Import facts
            FactRemainsImporter factClass = new FactRemainsImporter();

            // Remains
            factClass.impFactMovementsNewUpd();
            factClass.impFactMovementsDel();
            factClass.fromHistoryToRemains();
        }

        private void btnImportFactPrices_Click(object sender, RoutedEventArgs e)
        {
            // Import facts
            FactPricesImporter factClass = new FactPricesImporter();

            // Prices
            factClass.impFactPrices();
            factClass.fromHistoryToPrices();
        }

        private void btnImportDimPOSRegisters_Click(object sender, RoutedEventArgs e)
        {
            // Import dimensions
            DimImporter dimClass = new DimImporter();

            // POS
            dimClass.impDimPOSRegisters();
        }

        private void btnImportFactPOS_Click(object sender, RoutedEventArgs e)
        {
            // Import facts
            FactPOSClass factClass = new FactPOSClass();

            // POS Data
            factClass.impFactPOS();
            factClass.impFactPOSReturn();
            factClass.impFactPOSDel();
        }

        private void btnImportAllFacts_Click(object sender, RoutedEventArgs e)
        {
            // Import facts
            FactRemainsImporter factClass = new FactRemainsImporter();

            // Remains
            factClass.impFactMovementsNewUpd();
            factClass.impFactMovementsDel();
            factClass.fromHistoryToRemains();

            // Import facts
            FactPricesImporter factClass1 = new FactPricesImporter();

            // Prices
            factClass1.impFactPrices();
            factClass1.impFactPricesDel();
            factClass1.fromHistoryToPrices();

            // Import facts
            FactPOSClass factClass2 = new FactPOSClass();

            // POS Data
            factClass2.impFactPOS();
            factClass2.impFactPOSReturn();
            factClass2.impFactPOSDel();

            // Materials In Production
            FactManufactureImporter factClass3 = new FactManufactureImporter();
            factClass3.impFactMaterialsInProduction();
            factClass3.impFactMaterialsInProductionDel();
            factClass3.fromFactMaterialsToRemains();
            factClass3.impFactManufacture();
            factClass3.impFactManufactureDel();

            // Documents
            DocProductionImporter docClass = new DocProductionImporter();
            docClass.impDocProduction();
            docClass.impDocProductionDel();
            docClass.impDocProductionCosts();
            docClass.impDocProductionCostsDel();

            // Sales
            DocSalesImporter docClass4 = new DocSalesImporter();
            docClass4.impDocSales();
            docClass4.impDocSalesDel();

        }

        private void btnImportDimPeople_Click(object sender, RoutedEventArgs e)
        {
            // Import dimensions
            DimImporter dimClass = new DimImporter();

            // People
            dimClass.impDimPeople();
        }

        private void btnImportFactMaterialInProd_Click(object sender, RoutedEventArgs e)
        {
            FactManufactureImporter factClass = new FactManufactureImporter();
            factClass.impFactMaterialsInProduction();
            factClass.impFactMaterialsInProductionDel();
            factClass.fromFactMaterialsToRemains();
        }

        private void btnImportDimSpec_Click(object sender, RoutedEventArgs e)
        {
            // Import dimensions
            DimImporter dimClass = new DimImporter();

            // Spec
            dimClass.impDimSpec();
        }

        private void btnImportFactProduction_Click(object sender, RoutedEventArgs e)
        {
            FactManufactureImporter factClass = new FactManufactureImporter();
            factClass.impFactManufacture();
            factClass.impFactManufactureDel();
        }

        private void btnImportDocProduction_Click(object sender, RoutedEventArgs e)
        {
            DocProductionImporter docClass = new DocProductionImporter();
            docClass.impDocProduction();
            docClass.impDocProductionDel();
            docClass.impDocProductionCosts();
            docClass.impDocProductionCostsDel();
        }

        private void btnImportDimOps_Click(object sender, RoutedEventArgs e)
        {
            // Import operations
            DimImporter dimClass = new DimImporter();

            // Ops
            dimClass.impDimTechOperations();
        }

        private void btnImportDimPartners_Click(object sender, RoutedEventArgs e)
        {
            // Import partners
            DimImporter dimClass = new DimImporter();

            // Pn
            dimClass.impDimPartners();
        }

        private void btnImportDocSales_Click(object sender, RoutedEventArgs e)
        {
            // Sales
            DocSalesImporter docClass = new DocSalesImporter();
            docClass.impDocSales();
            docClass.impDocSalesDel();
        }

        private void btnImportDocTransfersProduction_Click(object sender, RoutedEventArgs e)
        {
            FactManufactureDocs docClass = new FactManufactureDocs();
            docClass.impFactTransferToProduction();
            docClass.impFactTransferToProductionDel();
            docClass.impFactTransferFromProduction();
            docClass.impFactTransferFromProductionDel();
        }


    } // public partial class MainWindow : Window
}
