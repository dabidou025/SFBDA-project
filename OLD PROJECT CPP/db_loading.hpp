#include <fstream>
#include "function.hpp"

using namespace std;

int loading(string file_name, Table *table) // modification des arguments, la table devient un pointeur
{
   string line;
   string delimiter = "|";
   int pos = 0;
   vector<string> temp;
   ifstream file(file_name);
   if (file.is_open())
   {
      getline(file, line);
      while (!file.eof())
      {
         pos = line.find(delimiter);
         while (pos > 0)
         {
            temp.push_back(line.substr(0, pos));
            line.erase(0, pos + delimiter.length());
            pos = line.find(delimiter);
         }
         getline(file, line);
         (*table).add_row(temp);
         temp.clear();
      }
      file.close();
   }

   return 0;
}