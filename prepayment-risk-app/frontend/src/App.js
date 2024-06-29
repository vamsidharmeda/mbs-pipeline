import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';

const API_ENDPOINT = 'http://localhost:8000/prepayment-data';

function App() {
  const [prepaymentData, setPrepaymentData] = useState([]);
  const [newData, setNewData] = useState({ month: '', prepayment_rate: '' });

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      const response = await axios.get(API_ENDPOINT);
      setPrepaymentData(response.data);
    } catch (error) {
      console.error('Error fetching data:', error);
    }
  };

  const handleInputChange = (e) => {
    setNewData({ ...newData, [e.target.name]: e.target.value });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      await axios.post(API_ENDPOINT, {
        month: newData.month,
        prepayment_rate: parseFloat(newData.prepayment_rate),
      });
      setNewData({ month: '', prepayment_rate: '' });
      fetchData();
    } catch (error) {
      console.error('Error adding data:', error);
    }
  };

  const handleDelete = async (id) => {
    try {
      await axios.delete(`${API_ENDPOINT}/${id}`);
      fetchData();
    } catch (error) {
      console.error('Error deleting data:', error);
    }
  };

  return (
    <div className="App" style={{ padding: '20px' }}>
      <h1>Prepayment Risk Analytics</h1>
      
      <LineChart width={600} height={300} data={prepaymentData}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="month" />
        <YAxis />
        <Tooltip />
        <Legend />
        <Line type="monotone" dataKey="prepayment_rate" stroke="#8884d8" />
      </LineChart>

      <h2>Add New Data</h2>
      <form onSubmit={handleSubmit}>
        <input
          type="text"
          name="month"
          value={newData.month}
          onChange={handleInputChange}
          placeholder="Month (YYYY-MM)"
          required
        />
        <input
          type="number"
          name="prepayment_rate"
          value={newData.prepayment_rate}
          onChange={handleInputChange}
          placeholder="Prepayment Rate"
          step="0.01"
          required
        />
        <button type="submit">Add Data</button>
      </form>

      <h2>Data Table</h2>
      <table>
        <thead>
          <tr>
            <th>Month</th>
            <th>Prepayment Rate</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {prepaymentData.map((item) => (
            <tr key={item.id}>
              <td>{item.month}</td>
              <td>{item.prepayment_rate}</td>
              <td>
                <button onClick={() => handleDelete(item.id)}>Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;
