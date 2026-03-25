# data_engineering/ingestion/macro_energy_extractor.py


import json
import os
from bs4 import BeautifulSoup
import pdfplumber
from base_client import StealthClient

class InstitutionalDataExtractor:
    def __init__(self, storage_dir: str, proxy: str = None):
        self.client = StealthClient(proxy_url=proxy)
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)

    def extract_cbn_mpc_communiques(self):
        """
        Scrapes the Central Bank of Nigeria for the latest Monetary Policy decisions.
        Captures both the lead sentence and the immediate decision details that follow.
        """
        url = "https://www.cbn.gov.ng/MonetaryPolicy/decisions.html"
        html_content = self.client.fetch_get(url).text
        soup = BeautifulSoup(html_content, "html.parser")

        decisions = []
        marker = "The Committee decided at the"

        # Find each MPC decision heading and then collect the nearby decision text
        for paragraph in soup.find_all("p"):
            text = paragraph.get_text(strip=True)
            print("--------------------------------")
            print(text)
            print("--------------------------------")
            if marker in text and "meeting of the MPC held" in text:
                block_lines = [text]

                # Walk forward through sibling elements to capture the concrete decisions
                for sibling in paragraph.find_next_siblings():
                    # Stop when we hit a new structural section (e.g. a header or unrelated block)
                    if sibling.name not in ("p", "ul", "ol"):
                        break

                    sib_text = sibling.get_text(" ", strip=True)
                    if not sib_text:
                        continue

                    # Stop if we've reached the next MPC decision heading
                    if marker in sib_text and "meeting of the MPC held" in sib_text:
                        break

                    block_lines.append(sib_text)

                    # Defensive cap so we don't accidentally consume the entire page
                    if len(block_lines) >= 6:
                        break

                decisions.append("\n  ".join(block_lines))

        output_path = os.path.join(self.storage_dir, "cbn_mpc_latest.md")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("# Central Bank of Nigeria - Recent MPC Decisions\n\n")
            for d in decisions:
                f.write(f"- {d}\n\n")
        print(f"Extracted {len(decisions)} CBN MPC records to {output_path}.")

    def extract_dmo_debt_statistics(self):
        """
        Locates and downloads the latest Public Debt Statistical Bulletins from the DMO.
        """
        url = "https://www.dmo.gov.ng/publications/other-publications/nigeria-s-public-debt-statistical-bulletin"
        html = self.client.fetch_get(url).text
        soup = BeautifulSoup(html, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            if 'download' in link['href'].lower() and 'bulletin' in link.get_text(strip=True).lower():
                title = link.get_text(strip=True).replace(" ", "_").replace("/", "-")
                full_url = f"https://www.dmo.gov.ng{link['href']}"
                file_path = os.path.join(self.storage_dir, f"DMO_{title}.pdf")
                
                print(f"Downloading DMO Data: {title}")
                self.client.download_file(full_url, file_path)
                
                # Automatically parse the downloaded PDF into Markdown
                self.parse_local_pdf_to_markdown(file_path)

    def parse_local_pdf_to_markdown(self, file_path: str) -> str:
        """
        Converts downloaded PDFs (e.g., NBS CPI reports, DMO bulletins) into Markdown, 
        preserving table structures for superior LLM semantic chunking.
        """
        md_text = f"# Parsed Document: {os.path.basename(file_path)}\n\n"
        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    # Extract raw narrative text
                    text = page.extract_text()
                    if text:
                        md_text += f"{text}\n\n"
                    
                    # Extract and format complex tables as Markdown
                    tables = page.extract_tables()
                    for table in tables:
                        if not table: continue
                        for i, row in enumerate(table):
                            # Clean newline characters within cells
                            cleaned_row = [str(cell).replace('\n', ' ') if cell else "" for cell in row]
                            md_text += "| " + " | ".join(cleaned_row) + " |\n"
                            # Add markdown header separator after the first row
                            if i == 0:
                                md_text += "|" + "|".join(["---"] * len(row)) + "|\n"
                        md_text += "\n"
                        
            # Save the markdown representation next to the PDF
            md_path = file_path.replace('.pdf', '.md')
            with open(md_path, 'w', encoding='utf-8') as f:
                f.write(md_text)
            print(f"Successfully parsed {file_path} into Markdown.")
            return md_text
        except Exception as e:
            print(f"Error parsing PDF {file_path}: {e}")
            return ""
        
if __name__ == "__main__":
    extractor = InstitutionalDataExtractor(storage_dir="data_engineering/ingestion/data")
    extractor.extract_cbn_mpc_communiques()
    extractor.extract_dmo_debt_statistics()