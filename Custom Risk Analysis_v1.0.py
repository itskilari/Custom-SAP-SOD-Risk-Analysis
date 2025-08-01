import pandas as pd
from pathlib import Path
import logging
from typing import Dict, List, Set
from dataclasses import dataclass
import warnings

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Config:
    """Configuration class for file paths and critical functions."""
    excel_path: str = "Enter Ruleset/Report excel path"
    output_path: str = "C:\\Users\\<User>\\Desktop\\SOD\\Report.xlsx"
    critical_functions: List[str] = None
    
    def __post_init__(self):
        if self.critical_functions is None:
            self.critical_functions = [
                'BS15', 'BS16', 'BS17', 'BS20', 'BS18', 'BS19', 'CA01',
                'FI10', 'FI11', 'FI12', 'HN03', 'HN04', 'HN05', 'HN02',
                'HN13', 'HN14', 'HN16', 'HN17', 'HN18', 'HR06', 'HR07', 
                'MM09', 'MM10', 'PM01', 'PP03', 'PR09', 'PR10', 'PS04', 
                'PS05', 'SD08', 'SD09'
            ]

class SODAnalyzer:
    """Segregation of Duties (SOD) Risk Analysis Tool."""
    
    def __init__(self, config: Config):
        self.config = config
        self.dataframes = {}
        self.processed_data = {}
        
    def load_data(self) -> None:
        """Load all required Excel sheets into memory."""
        try:
            excel_path = Path(self.config.excel_path)
            if not excel_path.exists():
                raise FileNotFoundError(f"Excel file not found: {excel_path}")
            
            # Load all sheets at once
            sheet_mapping = {
                'main': (0, None),  # (sheet_name_or_index, index_col)
                'function_actions': ('Function Actions', None),
                'action_function': ('Action Function', None),
                'function_risks': ('FunctionRisk', None),
                'risk_library': ('Risk Library', None)
            }
            
            for key, (sheet, index_col) in sheet_mapping.items():
                self.dataframes[key] = pd.read_excel(
                    excel_path, 
                    sheet_name=sheet, 
                    index_col=index_col
                )
                logger.info(f"Loaded {key} sheet with {len(self.dataframes[key])} rows")
                
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def preprocess_data(self) -> None:
        """Preprocess data for efficient lookups."""
        # Create efficient lookup dictionaries
        df_main = self.dataframes['main']
        
        # Group roles and their T-codes
        self.processed_data['role_tcodes'] = (
            df_main.groupby('Single roles')['T-code']
            .apply(set)
            .to_dict()
        )
        
        # Create function-action mappings
        df_action_func = self.dataframes['action_function']
        self.processed_data['action_functions'] = (
            df_action_func.groupby('Action')['Function']
            .apply(list)
            .to_dict()
        )
        
        # Create function-risk mappings
        df_func_risks = self.dataframes['function_risks']
        self.processed_data['function_risks'] = (
            df_func_risks.groupby('Function')[['Risk', 'RFunctions']]
            .apply(lambda x: x.to_dict('records'))
            .to_dict()
        )
        
        # Create risk-functions mappings
        self.processed_data['risk_functions'] = (
            df_func_risks.groupby('Risk')['RFunctions']
            .apply(list)
            .to_dict()
        )
        
        # Create function-actions reverse mapping
        df_func_actions = self.dataframes['function_actions']
        self.processed_data['function_actions'] = (
            df_func_actions.groupby('Function')['Action']
            .apply(list)
            .to_dict()
        )
        
        # Create risk library lookup
        df_risk_lib = self.dataframes['risk_library']
        self.processed_data['risk_details'] = df_risk_lib.set_index('Risk').to_dict('index')
        self.processed_data['function_details'] = df_risk_lib.set_index('Function').to_dict('index')
        
        logger.info("Data preprocessing completed")
    
    def analyze_role_risks(self, role: str, role_tcodes: Set[str]) -> List[Dict]:
        """Analyze risks for a specific role."""
        results = []
        
        for tcode in role_tcodes:
            if tcode not in self.processed_data['action_functions']:
                continue
                
            functions = self.processed_data['action_functions'][tcode]
            
            for function in functions:
                if function not in self.processed_data['function_risks']:
                    continue
                    
                # Check if critical function
                if function in self.config.critical_functions:
                    results.extend(self._handle_critical_function(role, tcode, function))
                else:
                    results.extend(self._handle_regular_function(role, tcode, function, role_tcodes))
        
        return results
    
    def _handle_critical_function(self, role: str, tcode: str, function: str) -> List[Dict]:
        """Handle critical function analysis."""
        results = []
        
        for risk_info in self.processed_data['function_risks'][function]:
            risk = risk_info['Risk']
            risk_details = self.processed_data['risk_details'].get(risk, {})
            func_details = self.processed_data['function_details'].get(function, {})
            
            result = {
                '0Roles': role,
                '1Tcode': tcode,
                '2Risks': risk,
                '3Risk Description': risk_details.get('Risk description', ''),
                '4Func': function,
                '5Function description': func_details.get('Function description', ''),
                '6CTcode': 'CRITICAL',
                '7CFunc': 'CRITICAL',
                '8ConFunction description': 'CRITICAL',
                '9Risk Description': risk_details.get('Risk type', ''),
                '10Risk Description': risk_details.get('Priority', '')
            }
            results.append(result)
            
            logger.info(f"{role} {tcode} - {risk} {function} (CRIT)")
        
        return results
    
    def _handle_regular_function(self, role: str, tcode: str, function: str, role_tcodes: Set[str]) -> List[Dict]:
        """Handle regular function analysis for conflicts."""
        results = []
        
        for risk_info in self.processed_data['function_risks'][function]:
            risk = risk_info['Risk']
            conflict_function = risk_info['RFunctions']
            
            if conflict_function == function:
                continue
                
            # Get conflicting T-codes
            conflict_tcodes = self.processed_data['function_actions'].get(conflict_function, [])
            
            for conflict_tcode in conflict_tcodes:
                if conflict_tcode in role_tcodes and conflict_tcode != tcode:
                    risk_details = self.processed_data['risk_details'].get(risk, {})
                    func_details = self.processed_data['function_details'].get(function, {})
                    conflict_func_details = self.processed_data['function_details'].get(conflict_function, {})
                    
                    result = {
                        '0Roles': role,
                        '1Tcode': tcode,
                        '2Risks': risk,
                        '3Risk Description': risk_details.get('Risk description', ''),
                        '4Func': function,
                        '5Function description': func_details.get('Function description', ''),
                        '6CTcode': conflict_tcode,
                        '7CFunc': conflict_function,
                        '8ConFunction description': conflict_func_details.get('Function description', ''),
                        '9Risk Description': risk_details.get('Risk type', ''),
                        '10Risk Description': risk_details.get('Priority', '')
                    }
                    results.append(result)
                    
                    logger.info(f"{role} {tcode} - {risk} {function} {conflict_function} {conflict_tcode}")
        
        return results
    
    def run_analysis(self) -> pd.DataFrame:
        """Run the complete SOD analysis."""
        logger.info("Starting SOD analysis...")
        
        all_results = []
        single_roles = set(self.dataframes['main']['Single roles'].tolist())
        
        for role in single_roles:
            role_tcodes = self.processed_data['role_tcodes'].get(role, set())
            role_results = self.analyze_role_risks(role, role_tcodes)
            all_results.extend(role_results)
        
        # Create results DataFrame
        if all_results:
            results_df = pd.DataFrame(all_results)
            results_df = results_df.fillna("CRITICAL")
        else:
            results_df = pd.DataFrame()
            logger.warning("No results found in analysis")
        
        logger.info(f"Analysis completed. Found {len(results_df)} risk instances")
        return results_df
    
    def export_results(self, results_df: pd.DataFrame) -> None:
        """Export results to Excel file."""
        try:
            output_path = Path(self.config.output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                results_df.to_excel(writer, sheet_name='SOD_Analysis', index=False)
                
                # Add some formatting
                worksheet = writer.sheets['SOD_Analysis']
                for column in worksheet.columns:
                    max_length = max(len(str(cell.value or '')) for cell in column)
                    worksheet.column_dimensions[column[0].column_letter].width = min(max_length + 2, 50)
            
            logger.info(f"Results exported to: {output_path}")
            
        except Exception as e:
            logger.error(f"Error exporting results: {e}")
            raise

def main():
    """Main execution function."""
    # Suppress pandas warnings
    warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)
    
    # Configuration
    config = Config(
        excel_path="Enter Ruleset/Report excel path",  # Update this path
        output_path="C:\\Users\\<User>\\Desktop\\SOD\\Report.xlsx"  # Update this path
    )
    
    try:
        # Initialize analyzer
        analyzer = SODAnalyzer(config)
        
        # Load and preprocess data
        analyzer.load_data()
        analyzer.preprocess_data()
        
        # Run analysis
        results = analyzer.run_analysis()
        
        # Export results
        if not results.empty:
            analyzer.export_results(results)
            print(f"Analysis complete! Found {len(results)} risk instances.")
        else:
            print("No risks found in the analysis.")
            
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise

if __name__ == "__main__":
    main()
