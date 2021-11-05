#include "table.hpp"

int sum(string query_col_name, Table t)
{
    int index = -1;
    for (int i = 0; i < t.m_cols_names.size(); i++) {
        if (query_col_name == t.m_cols_names[i]) {
            index = i;
            break;
        }
    }
    
    if ((t.m_cols_types[index] == ("Integer")) || (t.m_cols_types[index] == ("Float")))
    {
        //cout << "La somme de la colonne : " << query_col_name << " est egale a : " << t.m_cols[index]->sum() << endl;
        return t.m_cols[index]->sum();
    }
    else
    {
        cout << "On ne peux sommer le type de cette colonne" << endl;
    }
    return 0;
}

int avg(string query_col_name, Table t)
{
    int index = -1;
    for (int i = 0; i < t.m_cols_names.size(); i++) {
        if (query_col_name == t.m_cols_names[i]) {
            index = i;
            break;
        }
    }
    
    if ((t.m_cols_types[index] == ("Integer")) || (t.m_cols_types[index] == ("Float")))
    {
        //cout << "La moyenne de la colonne : " << query_col_name << " est egale a : " << t.m_cols[index]->sum() << endl;
        return t.m_cols[index]->sum() / t.m_cols[index]->get_size();
    }
    else
    {
        cout << "On ne peux faire la moyenne avec le type de cette colonne" << endl;
    }
    return 0;
}

Table projection(vector<string> query, Table t)
{
    Table res;
    vector<string> col_typ;
    //On récupère les attributs de notre table pour construire la nouvelle
    for (int i = 0; i < t.m_cols.size(); i++) {
        for (int j = 0; j < query.size(); j++) {
            if (t.m_cols_names[i] == query[j]) {
                res.m_cols_types.push_back(t.m_cols_types[i]);
                res.m_cols_names.push_back(t.m_cols_names[i]);
                res.m_cols.push_back(t.m_cols[i]);
            }
        }
    }
    if (res.m_cols.size() > 0)
        res.n_row = t.n_row;
    return res;
}

void distinct(Table &t)
{
    int nb_col=t.m_cols_types.size();
    for(int i=0;i<nb_col;i++)
    {
        t.m_cols[i]->distinct();
    }
}