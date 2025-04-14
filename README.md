# 📦 Automação de Estoque - Marchon

Este projeto automatiza o processo de atualização de estoque para a empresa **Marchon**. Ele conecta-se a um servidor **SFTP**, faz o download do arquivo de estoque, cruza as informações com uma planilha de referência local e envia os dados atualizados para a **API do Bling**, registrando os logs e atualizando os arquivos de controle automaticamente no repositório GitHub.

---

## 🚀 Funcionalidades

- ✅ Conexão com servidor SFTP para download automático do arquivo `estoque_disponivel_10.csv`
- ✅ Leitura e processamento da planilha `Estoque_10.xlsx` com informações de produtos
- ✅ Cruzamento automático entre produtos do estoque e os produtos cadastrados
- ✅ Geração da planilha `resultado_correspondencias_10.xlsx` com os dados prontos para envio
- ✅ Envio automático de dados para a API do Bling com autenticação via **OAuth2 (Bearer Token)**
- ✅ Registro de logs de atividade
- ✅ Commit e push automático do resultado para o GitHub

---

## 🧰 Tecnologias Utilizadas

- Python 3.10+
- `pandas`
- `requests`
- `paramiko`
- `openpyxl`
- `logging`
- `smtplib` (para envio de e-mails de notificação)
- GitHub Actions (opcional para CI/CD)

---
