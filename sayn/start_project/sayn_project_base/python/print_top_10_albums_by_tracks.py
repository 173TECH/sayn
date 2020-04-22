#IMPORTANT: for python tasks to be able to execute, you neeed to have an __init__.py file into the python folder so it is treated as a package
#here we define a python task
#python tasks inherit from the sayn PythonTask
from sayn import PythonTask

#a python task needs to implement two functions
#setup() wich operates necessary setup and returns self.ready() to indicate the task is ready to be ran
#run() which executes the task and returns self.finished() to indicate the task has finished successfully
class TopAlbumsPrinter(PythonTask):
    #here we are not doing anything for setup, just displaying the usage of self.failed()
    #in order to inform sayn that the task has failed, you would return self.failed()
    #note that self.failed() can also be used for run()
    #please note that setup() needs to follow the method's signature. Therefore it needs to be set as setup(self).
    def setup(self):
        #code doing setup
        err = False
        if err:
            return self.failed()
        else:
            return self.ready()

    #here we define the code that will be executed at run time
    #please note that run() needs to follow the method's signature. Therefore it needs to be set as run(self).
    def run(self):
        #we can access the project parameters via the sayn_config attribute
        sayn_params = self.sayn_config.parameters
        #we use the config parameters to make the query dynamic
        #the query will therefore use parameters of the profile used at run time
        q = '''
            SELECT tpao.*

            FROM {schema_models}.{table_prefix}tracks_per_album_ordered tpao

            LIMIT 10
            ;
            '''.format(
                schema_models=sayn_params['schema_models'],
                table_prefix=sayn_params['table_prefix']
            )

        #the python task has the project's default_db connection object as an attribute.
        #this attribute has an number of method including select() to run a query and return results.
        #please see the documentation for more details on the API
        r = self.default_db.select(q)

        print('Printing top 10 albums by number of tracks:')
        for i in range(10):
            print('#{rank}: {album}, {n} tracks.'.format(rank=i+1, album=r[i]['album_name'], n=r[i]['n_tracks']))

        return self.finished()
