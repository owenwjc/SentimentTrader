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

document.querySelectorAll('#delete-button').forEach(deleteButton =>

    deleteButton.addEventListener('click', _ => {
        fetch('/names',{
            method: 'delete',
            headers: { 'Content-Type': 'application/json'},
            body: JSON.stringify({
                _id: deleteButton.value
            })
        }).then(res=> {
            if(res.ok) return res.json()
        }).then(data => {
            window.location.reload()
    })
}))