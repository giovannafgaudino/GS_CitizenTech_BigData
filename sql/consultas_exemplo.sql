-- Consultas que o MVP CidadeViva (e o time de BI) executa sobre o Data Warehouse.

-- 1) Ranking de demanda por subprefeitura e categoria (alimenta o mapa de calor do app)
SELECT s.nome_subprefeitura, c.nome AS categoria, SUM(m.qtd_solicitacoes) AS solicitacoes,
       ROUND(AVG(m.taxa_resolucao), 3) AS taxa_resolucao_media
FROM mart_demanda_subprefeitura_mes m
JOIN dim_subprefeitura s USING (subprefeitura_id)
JOIN dim_categoria c USING (categoria_id)
GROUP BY 1, 2 ORDER BY solicitacoes DESC LIMIT 10;

-- 2) Fila de revisão humana da triagem por IA (human-in-the-loop)
SELECT r.reporte_id, c.nome AS categoria_sugerida, r.confianca_ia, r.descricao
FROM fato_reporte r LEFT JOIN dim_categoria c USING (categoria_id)
WHERE r.requer_revisao_humana = 1 ORDER BY r.confianca_ia ASC LIMIT 20;

-- 3) Reportes do app em áreas com histórico alto de demanda na Prefeitura (prioridade para o RPA enviar primeiro)
SELECT r.reporte_id, s.nome_subprefeitura, c.nome AS categoria, d.qtd_solicitacoes AS demanda_historica
FROM fato_reporte r
JOIN dim_subprefeitura s USING (subprefeitura_id)
JOIN dim_categoria c USING (categoria_id)
JOIN (SELECT subprefeitura_id, categoria_id, SUM(qtd_solicitacoes) qtd_solicitacoes
      FROM mart_demanda_subprefeitura_mes GROUP BY 1, 2) d USING (subprefeitura_id, categoria_id)
WHERE r.status = 'aberto' ORDER BY d.qtd_solicitacoes DESC LIMIT 20;

-- 4) Qualidade do modelo de IA por categoria
SELECT c.nome, SUM(t.qtd_reportes) reportes, ROUND(AVG(t.confianca_media), 3) confianca,
       ROUND(1.0 * SUM(t.qtd_revisao_humana) / SUM(t.qtd_reportes), 3) pct_revisao
FROM mart_triagem_ia t JOIN dim_categoria c USING (categoria_id) GROUP BY 1 ORDER BY pct_revisao DESC;
