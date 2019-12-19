# Asgard Events Indexer

Projeto para indexar e guardar todos os eventos de um cluster Asgard, já separado por namespace (Account).

O objetivo principal do projeto e ter sempre atualizado uma lista de todas as tarefas rodando no cluster e todos
os agentes (servidores) que fazem parte desse cluster.


## Eventos indexados

Por equanto estamos indexando eventos referentes a:

- Tasks
- Agents


### Tasks events

Aqui teremos eventos relacionado às tarefas que estão rodando do cluster. Tasks adicionadas, removidas, terminadas, etc.
Cada evento conterá um campo dizendo a qual namespace asgard (Account) aquela task pertence.

### Agents events

Aqui teremos tudo sobre os agentes (servidres) que fazem parte do cluster. Cada evento também conterá o namespace que esse agente pertence.
Teremos eventos de Agent adicionado e Agent removido.



# Configurações

## ENV Vars

 - `INDEXER_MESOS_MASTER_URLS`: Lista de endereços dos nós do cluster de mesos;
