#include "col.hpp"

using namespace std;

class Table
{
public:
  vector<string> m_cols_names;
  vector<string> m_cols_types;
  vector<ColBase*> m_cols;
  int n_row;

  Table() {}

  Table(vector<string> cols_names, vector<string> cols_types)
  {
    n_row = 0;
    m_cols_names = cols_names;
    m_cols_types = cols_types;

    for (int i = 0; i < m_cols_types.size(); i++)
    {
      if (cols_types[i] == "Integer")
        m_cols.push_back(new ColInt());
      else if (cols_types[i] == "Float")
        m_cols.push_back(new ColFloat());
      else
        m_cols.push_back(new ColChar(stoi(cols_types[i])));
    }
  }

  void add_row(vector<string> data)
  {
    for (int i = 0; i < (int)m_cols.size(); i++)
    {
      m_cols[i]->add_value(data[i]);
    }
    n_row++;
  }

  void print() // création de la méthode print pour l'affichage de la table
  {
    for (int i = 0; i < m_cols_names.size(); i++)
    {
      cout << m_cols_names[i] << " - ";
    }
    cout << endl;

    int nb_col = m_cols.size();
    for (int l = 0; l < m_cols[0]->get_size(); l++)
    {
      for (int i = 0; i < nb_col; i++)
      {
        m_cols[i]->get_value(l);
        cout << " - ";
      }
      cout << endl;
    }
  }
};
