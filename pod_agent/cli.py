# pod_agent/cli.py
from dotenv import load_dotenv
from . import database, logic

# Load environment variables for the CLI tool
load_dotenv()
CURRENT_USER = "jperk_cli"

def main():
    """The main function to run the command-line interface."""
    database.init_db_and_seed()
    print(f"--- CPG POD Tracker CLI (v1.0) ---\nLogged in as: {CURRENT_USER}")
    print("Commands: 'bulk_add <file.csv>', 'export <file.xlsx>', 'reset', 'exit'")
    
    # We don't need conversational memory for this simple CLI version
    while True:
        user_input = input("\n> ")
        if user_input.lower() in ["exit", "quit"]:
            break

        try:
            command_parts = user_input.split()
            intent = logic.classify_intent(user_input)

            if intent == 'bulk_add':
                if len(command_parts) > 1:
                    filename = command_parts[1]
                    success_count, errors = logic.process_bulk_file(filename, CURRENT_USER)
                    print(f"\n--- Bulk Add Complete ---")
                    print(f"✅ Successfully logged {success_count} transactions.")
                    if errors:
                        print(f"❌ Skipped {len(errors)} transactions with errors:")
                        for error in errors:
                            print(f"  - {error}")
                else:
                    print("❗️Usage: bulk_add <filename.csv>")

            elif intent == 'export':
                if len(command_parts) > 1:
                    filename = command_parts[1]
                    export_df = logic.generate_export_dataframe()
                    if export_df is not None:
                        export_df.to_excel(filename, sheet_name='POD_Tracker')
                        print(f"✅ Successfully exported POD Tracker to '{filename}'")
                    else:
                        print("❌ No data to export.")
                else:
                    print("❗️Usage: export <filename.xlsx>")
            
            # This CLI doesn't support conversational logging, only direct commands.
            # We can add that back later if needed.

            elif intent == 'query_data':
                plan = logic.generate_query_plan(user_input)
                results = logic.execute_query_plan(plan)
                print("\n--- Query Results ---")
                if results is None or results.empty:
                    print("No matching records found.")
                else:
                    print(results.to_string())
                print("---------------------")

            else:
                print("❓ Command not recognized. Please use bulk_add, export, or ask a question.")

        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
