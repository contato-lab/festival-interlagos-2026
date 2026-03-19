"""
Script para gerar o refresh_token com escopos COMBINADOS:
  - Google Ads API (adwords)
  - Google Analytics Data API (analytics.readonly)

Execute UMA VEZ localmente. O refresh_token gerado vai para o GitHub Secret
GOOGLE_ADS_REFRESH_TOKEN (usado por ambos os scripts: google ads e ga4).

Dependências: pip install google-auth-oauthlib
"""

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/adwords",
    "https://www.googleapis.com/auth/analytics.readonly",
]
CLIENT_SECRETS_FILE = "client_secret.json"

def main():
    flow = InstalledAppFlow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES
    )

    # Abre o navegador para autorização (force consent para garantir refresh_token)
    credentials = flow.run_local_server(port=8090, prompt="consent")

    print("\n" + "="*60)
    print("SALVE ESSAS CREDENCIAIS NOS GITHUB SECRETS:")
    print("="*60)
    print(f"CLIENT_ID:     {credentials.client_id}")
    print(f"CLIENT_SECRET: {credentials.client_secret}")
    print(f"REFRESH_TOKEN: {credentials.refresh_token}")
    print("="*60)
    print("\nAtualize o secret GOOGLE_ADS_REFRESH_TOKEN no GitHub com o valor acima.")

if __name__ == "__main__":
    main()
