const update = document.querySelector('#update-button')

const data = {name: 'David',
              button: 'Bullish'}

update.addEventListener('click', _ =>{
    fetch('/names',{
        method: 'put',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(data)
    })
})