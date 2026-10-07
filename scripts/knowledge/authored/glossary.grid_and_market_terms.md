# Glossary: grid control areas and market zones

TSO and control area names, the same in MaStR, SMARD and redispatch: `fifty_hertz`, `amprion`, `tennet_de` and `transnetbw`. Redispatch can also name a foreign TSO, such as `pse_poland`, `apg_austria` or `swissgrid`.

The DE-LU bidding zone is a separate value. It is not a control area.

Two kinds of geography stay separate. Administrative geography is `dim_geography`: nation, Bundesland, Regierungsbezirk, Kreis and Gemeinde, keyed by AGS or ARS. Market geography is `dim_market`: the bidding zone, the four control areas and the TSO entities.

A market zone or control area is a regulatory construct. It is never forced into the administrative hierarchy. SMARD region, the redispatch TSOs and the MaStR control zone map to `dim_market`. One row may carry both an administrative and a market attribution. A MaStR generation unit does.
