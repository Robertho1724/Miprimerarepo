import unittest
from airflow.models import DagBag

class TestHelloAirflowDAG(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag()

    def test_dag_loaded(self):
        """Comprueba si el DAG está correctamente en el DagBag"""
        dag = self.dagbag.get_dag(dag_id='ejemplo_unittest')
        self.assertDictEqual(self.dagbag.import_errors, {}, msg="DAGs con errores al cargar")
        self.assertIsNotNone(dag, msg="DAG no está disponible en DagBag")

        # RESULTADO: SI ENCONTRARÁ EL DAG


    def test_contain_tasks(self):
        """Comprueba si todas las tareas están en el DAG"""
        dag = self.dagbag.get_dag(dag_id='ejemplo_unittest')
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertIn('greet', task_ids, msg="DAG no contiene la tarea 'greet'")

        # RESULTADO: SI ENCONTRARÁ TAREA



    def test_dependencies_of_greet_task(self):
        """Comprueba las dependencias de la tarea 'greet'"""
        dag = self.dagbag.get_dag(dag_id='ejemplo_unittest')
        greet_task = dag.get_task('greet')

        upstream_task_ids = list(map(lambda task: task.task_id, greet_task.upstream_list))
        downstream_task_ids = list(map(lambda task: task.task_id, greet_task.downstream_list))

        self.assertEqual(len(upstream_task_ids), 0, msg="greet tiene dependencias upstream incorrectas")
        self.assertEqual(len(downstream_task_ids), 0, msg="greet tiene dependencias downstream incorrectas")

        # RESULTADO: SIN PROBLEMAS



#EXTRA PRUEBAS:
# - VERIFICAR ATRIBUTOS DEL DAG
# - VERIFICAR CONEXION
# - VERIFICAR PARALELISMO
# - VERIFICAR TIEMPO DE EJECUCION
# - VERIFICAR FUNCIONES EJECUTADAS

if __name__ == '__main__':
    unittest.main()