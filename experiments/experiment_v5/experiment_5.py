import json
import datetime
import sys

def generate_json(bad_site, good_site, peers, bad_origin, good_origin, prepend_count=5, num_rounds=7, email=None):
    """
    Gera o JSON conforme especificado.

    Args:
        bad_site: Site com origem inválida
        good_site: Site com origem válida
        peers: Lista de peers para o bad_site
        bad_origin: ASN de origem para o bad_site
        good_origin: ASN de origem para o good_site
        prepend_count: Número de vezes para repetir o ASN no prepend (padrão: 5)
        num_rounds: Número de rounds (padrão: 7)
        email: Email para incluir no JSON (padrão: marcelmendes@dcc.ufmg.br)
    """
    # Gerar data atual no formato dd/mm/YYYY
    current_date = datetime.datetime.now().strftime("%d/%m/%Y")

    # Email padrão se não especificado
    if email is None:
        email = "marcelmendes@dcc.ufmg.br"

    # Criar lista de prepend
    prepend_list = [good_origin] * prepend_count

    # Criar template para um round
    def create_round_template():
        return {
            "138.185.228.0/24": {
                "announce": [
                    {
                        "muxes": [bad_site],
                        "peers": peers,
                        "origin": bad_origin
                    }
                ]
            },
            "138.185.229.0/24": {
                "announce": [
                    {
                        "muxes": [bad_site],
                        "peers": peers,
                        "origin": bad_origin
                    }
                ]
            },
            "138.185.230.0/24": {
                "announce": [
                    {
                        "muxes": [bad_site],
                        "peers": peers,
                        "origin": bad_origin
                    },
                    {
                        "muxes": [good_site],
                        "origin": good_origin,
                        "prepend": prepend_list
                    }
                ]
            },
            "138.185.231.0/24": {
                "announce": [
                    {
                        "muxes": [good_site],
                        "origin": good_origin,
                        "prepend": prepend_list
                    }
                ]
            },
            "204.9.170.0/24": {
                "announce": [
                    {
                        "muxes": [bad_site],
                        "peers": peers,
                        "origin": bad_origin
                    },
                    {
                        "muxes": [good_site],
                        "origin": good_origin,
                        "prepend": prepend_list
                    }
                ]
            }
        }

    # Criar a estrutura principal
    result = {
        "email": email,
        "rounds": [create_round_template() for _ in range(num_rounds)],
        "description": f"Experiment 5 (traceroute-extended) {bad_site} (valid {good_origin}, invalid {bad_origin}) id: {current_date}"
    }

    return result

def parse_peers(peers_str):
    """
    Converte string de peers para lista de inteiros.
    Pode aceitar formatos como "191", "191,192,193" ou "[191,192,193]"
    """
    # Remover colchetes e espaços
    peers_str = peers_str.strip("[] ")
    if not peers_str:
        return []

    # Dividir por vírgula e converter para inteiros
    return [int(p.strip()) for p in peers_str.split(",") if p.strip()]

def main():
    if len(sys.argv) < 6:
        print("Uso: python experiment_5.py <bad_site> <good_site> <peers> <bad_origin> <good_origin>")
        print("")
        print("Argumentos:")
        print("  bad_site:    Site com origem inválida (ex: utah01)")
        print("  good_site:   Site com origem válida (ex: vtrmiami)")
        print("  peers:       Lista de peers (ex: 191 ou 191,192,193 ou [191,192,193])")
        print("  bad_origin:  ASN de origem para o bad_site (ex: 47065)")
        print("  good_origin: ASN de origem para o good_site (ex: 61574)")
        print("")
        print("Argumentos opcionais (após os obrigatórios):")
        print("  --prepend N:     Número de vezes para repetir no prepend (padrão: 5)")
        print("  --rounds N:      Número de rounds (padrão: 7)")
        print("  --email EMAIL:   Email para incluir no JSON")
        print("  --output FILE:   Arquivo de saída (padrão: stdout)")
        print("")
        print("Exemplos:")
        print("  python experiment_5.py utah01 vtrmiami 191 47065 61574")
        print("  python experiment_5.py utah01 vtrmiami \"191,192,193\" 47065 61574 --prepend 3 --rounds 4")
        print("  python experiment_5.py site1 site2 191 65001 65002 --email teste@exemplo.com --output config.json")
        sys.exit(1)

    # Argumentos obrigatórios
    bad_site = sys.argv[1]
    good_site = sys.argv[2]
    peers = parse_peers(sys.argv[3])
    bad_origin = int(sys.argv[4])
    good_origin = int(sys.argv[5])

    # Valores padrão para argumentos opcionais
    prepend_count = 5
    num_rounds = 7
    email = None
    output_file = None

    # Processar argumentos opcionais
    i = 6
    while i < len(sys.argv):
        if sys.argv[i] == "--prepend" and i + 1 < len(sys.argv):
            prepend_count = int(sys.argv[i + 1])
            i += 2
        elif sys.argv[i] == "--rounds" and i + 1 < len(sys.argv):
            num_rounds = int(sys.argv[i + 1])
            i += 2
        elif sys.argv[i] == "--email" and i + 1 < len(sys.argv):
            email = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--output" and i + 1 < len(sys.argv):
            output_file = sys.argv[i + 1]
            i += 2
        else:
            print(f"Argumento desconhecido: {sys.argv[i]}")
            sys.exit(1)

    # Gerar o JSON
    json_data = generate_json(
        bad_site=bad_site,
        good_site=good_site,
        peers=peers,
        bad_origin=bad_origin,
        good_origin=good_origin,
        prepend_count=prepend_count,
        num_rounds=num_rounds,
        email=email
    )

    # Converter para JSON formatado
    json_str = json.dumps(json_data, indent=4)

    # Salvar em arquivo ou imprimir
    if output_file:
        with open(output_file, 'w') as f:
            f.write(json_str)
        print(f"JSON salvo em: {output_file}")
    else:
        print(json_str)

if __name__ == "__main__":
    main()
